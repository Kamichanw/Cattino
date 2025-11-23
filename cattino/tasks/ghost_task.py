import math
import threading
import time
import torch

from cattino.platforms import current_platform
from cattino.tasks.interface import DeviceRequiredTask, TaskStatus
from cattino.constants import CATTINO_INVISIBLE_TASK_PREFIX

class GhostTask(DeviceRequiredTask):
    """
    A device "ghost" task that occupies a device by performing continuous matrix multiplications.
    This task is used to protect device resources.
    """

    def __init__(self, task_name: str | None = None) -> None:
        super().__init__(
            task_name=task_name,
            priority=1000000,
            requires_memory_per_device=0,
            min_devices=1,
        )

        self.do_ops: bool = False
        # single lock protecting allocation, tensor access and status
        self._lock = threading.Lock()
        # status protected by `self._lock`
        self._status: TaskStatus = TaskStatus.Waiting

        self._compute_thread: threading.Thread | None = None
        self._stop_event = threading.Event()

        self._tensors: tuple[torch.Tensor, torch.Tensor] | None = None
    
    @property
    def fullname(self) -> str:
        return f"{CATTINO_INVISIBLE_TASK_PREFIX}_ghost_{self.name}"

    @DeviceRequiredTask.requires_memory_per_device.setter
    def requires_memory_per_device(self, value: int | float) -> None:
        """Override the property setter to eagerly allocate when a device
        is already assigned. Allocation is guarded by `self._alloc_lock` to
        avoid races with the compute thread.
        """
        DeviceRequiredTask.requires_memory_per_device.__set__(self, value)

        with self._lock:
            try:
                total_bytes = int(self.requires_memory_per_device * 1024 * 1024)

                if total_bytes == 0 or self.device_id is None:
                    self._tensors = None
                    return

                device = torch.device(current_platform.device_type, self.device_id)

                bytes_for_pair = max(8, total_bytes)
                bytes_for_matrix = max(4, bytes_for_pair // 2)
                # float32 -> 4 bytes per element
                side = max(64, int(math.sqrt(bytes_for_matrix / 4)))

                A = torch.empty(
                    (side, side), dtype=torch.float32, device=device
                ).uniform_(-1.0, 1.0)
                B = torch.empty(
                    (side, side), dtype=torch.float32, device=device
                ).uniform_(-1.0, 1.0)
                self._tensors = (A, B)
            except Exception:
                self._status = TaskStatus.Failed

    @DeviceRequiredTask.min_devices.setter
    def min_devices(self, value: int) -> None:
        if value != 1:
            raise ValueError("GhostTask always requires exactly one device")
        self._min_devices = 1

    @property
    def device_id(self) -> int | None:
        if self.assigned_device_indices:
            return self.assigned_device_indices[0]
        return None

    def start(self) -> None:
        if not self.is_ready:
            raise RuntimeError(f"{self.name} is not ready to be executed.")
        self._stop_event.clear()
        self._compute_thread = threading.Thread(
            target=self._run, name=f"{self.name}-compute", daemon=True
        )
        self._compute_thread.start()

    def _run(self) -> None:
        """Compute thread: allocate matrices and run matmuls in a tight loop.

        The loop cooperatively checks `self._stop_event` and exits when it
        is set (either via `cancel()` or `terminate()`).
        """

        with self._lock:
            self._status = TaskStatus.Running
        try:
            # devices are assigned at this point
            assert self.device_id is not None

            device = torch.device(current_platform.device_type, self.device_id)
            repeat_count = 8
            while not self._stop_event.is_set():
                with self._lock:
                    local_pair = self._tensors
                if not self.do_ops or local_pair is None:
                    # not doing ops right now; sleep briefly to avoid busy spin
                    time.sleep(1)
                else:
                    A, B = local_pair

                    for _ in range(repeat_count):
                        _ = torch.matmul(A, B)
                    # synchronize once per batch to ensure kernels finish
                    if device.type == "cuda":
                        torch.cuda.synchronize(device)
                    else:
                        npu_mod = getattr(torch, "npu", None)
                        if npu_mod is not None and device.type.startswith("npu"):
                            npu_mod.synchronize()  # type: ignore

        except Exception:
            with self._lock:
                self._status = TaskStatus.Failed

    def wait(self, timeout: float | None = None) -> None:
        if self._compute_thread is not None:
            self._compute_thread.join(timeout)

    def cancel(self) -> None:
        if self.status in [TaskStatus.Done, TaskStatus.Failed]:
            return
        # signal compute thread to stop and wait briefly
        self._stop_event.set()
        if self._compute_thread is not None and self._compute_thread.is_alive():
            self._compute_thread.join(timeout=1.0)
        with self._lock:
            self._status = TaskStatus.Cancelled

    def resume(self) -> None:
        if self.status in [TaskStatus.Running, TaskStatus.Waiting]:
            return
        # prepare to run again
        self._compute_thread = None
        self._stop_event.clear()
        self._tensors = None
        with self._lock:
            self._status = TaskStatus.Waiting

    def terminate(self, force: bool = False) -> None:
        self._stop_event.set()
        if self._compute_thread is not None and self._compute_thread.is_alive():
            self._compute_thread.join(timeout=1.0)
        with self._lock:
            self._status = TaskStatus.Done

    @property
    def status(self) -> TaskStatus:
        with self._lock:
            return self._status

    def on_end(self) -> None:
        super().on_end()
        with self._lock:
            self._tensors = None
        self.release_devices()
