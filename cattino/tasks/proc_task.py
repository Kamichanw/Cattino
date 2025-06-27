import os
import shlex
import inspect
import subprocess
import multiprocessing
from typing import Any, Callable, Dict, Optional, Union, overload

from cattino.constants import CATTINO_RETRY_EXIT_CODE
from cattino.tasks.interface import TaskStatus, DeviceRequiredTask
from cattino.utils import Magics, get_cache_dir, open_redirected_stream


class ProcTask(DeviceRequiredTask):

    @overload
    def __init__(
        self,
        cmd: str,
        env: Optional[Dict[str, Any]] = None,
        task_name: Optional[str] = None,
        priority: int = 1,
        requires_memory_per_device: Union[int, float] = 0,
        min_devices: int = 1,
    ) -> None:
        """
        Initialize a ProcTask with a shell command.

        Args:
            cmd (str): A magic string representing the command to execute.
            env (Dict[str, Any], *optional*): Environment variables for the task.
            task_name (str, *optional*): Name of the task.
            priority (int, *optional*): Task priority.
            requires_memory_per_device (int or float): Memory required per device in MiB, or
                as a ratio of total memory (0-1).
            min_devices (int, *optional*): Minimum number of devices required.
        """
        ...

    @overload
    def __init__(
        self,
        func: Callable[[], None],
        env: Optional[Dict[str, Any]] = None,
        task_name: Optional[str] = None,
        priority: int = 1,
        requires_memory_per_device: Union[int, float] = 0,
        min_devices: int = 1,
    ) -> None:
        """
        Initialize a ProcTask with a Python callable.

        Args:
            func (Callable): A callable without any params to be executed in a new process.
            env (Dict[str, Any], *optional*): Environment variables for the task.
            task_name (str, *optional*): Name of the task.
            priority (int, *optional*): Task priority.
            requires_memory_per_device (int or float): Memory required per device in MiB, or
                as a ratio of total memory (0-1).
            min_devices (int, *optional*): Minimum number of devices required.
        """
        ...

    def __init__(  # type: ignore
        self,
        cmd_or_func: Union[str, Callable],
        env: Optional[Dict[str, Any]] = None,
        task_name: Optional[str] = None,
        priority: int = 1,
        requires_memory_per_device: Union[int, float] = 0,
        min_devices: int = 1,
    ) -> None:
        self._proc: Optional[Union[subprocess.Popen, multiprocessing.Process]] = None
        self._is_cancelled = False

        super().__init__(
            task_name=task_name,
            priority=priority,
            requires_memory_per_device=requires_memory_per_device,
            min_devices=min_devices,
        )

        if callable(cmd_or_func):
            self._target_fn = cmd_or_func
            if len(inspect.signature(cmd_or_func).parameters) > 0:
                raise ValueError(
                    "The argument function should not have any parameters, "
                    f"but got {len(inspect.signature(cmd_or_func).parameters)}"
                )
        else:
            self.cmd = cmd_or_func

        self.env = env
        self.min_devices = min_devices
        self.requires_memory_per_device = requires_memory_per_device

    @property
    def pid(self) -> Optional[int]:
        """
        Get the process ID of the task.

        Returns:
            Optional[int]: The process ID if the task is running; None otherwise.
        """
        if self._proc is None:
            return None
        return self._proc.pid

    def cancel(self):
        if self.status in [
            TaskStatus.Done,
            TaskStatus.Failed,
        ]:
            return

        self._is_cancelled = True

    @property
    def status(self) -> TaskStatus:
        if self._is_cancelled:
            return TaskStatus.Cancelled
        if self._proc is None:
            return TaskStatus.Waiting
        if isinstance(self._proc, subprocess.Popen):
            if self._proc.poll() is None:
                return TaskStatus.Running
            exitcode = self._proc.returncode
        else:
            if self._proc.exitcode is None:
                return TaskStatus.Running
            exitcode = self._proc.exitcode
        return TaskStatus.Done if exitcode == 0 else TaskStatus.Failed

    @DeviceRequiredTask.requires_memory_per_device.setter
    def requires_memory_per_device(self, value: Union[int, float]) -> None:
        if self.pid is not None:
            if any(
                self._allocator.get_proc_memory_usage(self.pid, device_id) > value
                for device_id in self.assigned_device_indices or []
            ):
                raise ValueError(
                    f"Task {self.name} requires more memory than the specified limit per device."
                )
        super(ProcTask, type(self)).requires_memory_per_device.__set__(self, value) # type: ignore

    @property
    def is_ready(self) -> bool:
        """
        Check whether the task is ready for execution.

        Returns:
            bool: True if the task is pending and device allocation is successful; False otherwise.
        """
        if self.status == TaskStatus.Waiting:
            return self.acquire_devices()
        return False

    def start(self) -> None:
        if not self.is_ready:
            raise RuntimeError(f"{self.name} is not ready to be executed.")

        is_cmd_task = hasattr(self, "cmd")
        task_env = self.env or os.environ
        cache_dir = get_cache_dir(self)
        merged_env = {
            **task_env,
            **self.visible_device_environ,
            "CATTINO_TASK_HOME": cache_dir,
        }
        self._stdout = open_redirected_stream(cache_dir, "stdout")
        self._stderr = open_redirected_stream(cache_dir, "stderr")
        if is_cmd_task:
            self.cmd = Magics.resolve(
                self.cmd,
                task_name=self.name,
                run_dir=get_cache_dir(""),
                fullname=self.fullname,
            )
            self._proc = subprocess.Popen(
                shlex.split(self.cmd),
                stdout=self._stdout,
                stderr=self._stderr,
                env=merged_env,
            )
        else:

            def target_wrapper():
                import sys

                sys.stdout = self._stdout
                sys.stderr = self._stderr
                os.environ.update(merged_env)
                return self._target_fn()

            self._proc = multiprocessing.Process(target=target_wrapper, name=self.name)
            self._proc.start()

    def wait(self, timeout: Optional[float] = None) -> None:
        if self.status == TaskStatus.Running:
            if isinstance(self._proc, subprocess.Popen):
                self._proc.wait(timeout)
                exitcode = self._proc.returncode
            elif isinstance(self._proc, multiprocessing.Process):
                self._proc.join(timeout)
                exitcode = self._proc.exitcode
            else:
                assert False, "Unknown process type"

            if exitcode == CATTINO_RETRY_EXIT_CODE:
                self.resume()
                # the status of the process has been changed
                # to waiting, the on_end will not be called, so we
                # need to call it manually
                self.on_end()

    def resume(self) -> None:
        if self.status not in [TaskStatus.Running, TaskStatus.Waiting]:
            self._proc = None
            self._is_cancelled = False

    def terminate(self, force: bool = False) -> None:
        if self.status == TaskStatus.Running:
            self._proc.kill() if force else self._proc.terminate()  # type: ignore

    def on_end(self) -> None:
        super().on_end()
        self._stdout.close()
        self._stderr.close()
        del self._stdout
        del self._stderr
