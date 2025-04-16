import os
import shlex
import inspect
import subprocess
import multiprocessing
import psutil
from typing import Any, Callable, Dict, List, Optional, Union, overload

from catin.tasks.interface import TaskStatus, DeviceRequiredTask
from catin.utils import Magics, get_cache_dir, open_redirected_stream


class ProcTask(DeviceRequiredTask):

    @overload
    def __init__(
        self,
        cmd: str,
        env: Optional[Dict[str, Any]] = None,
        task_name: Optional[str] = None,
        priority: int = 1,
        visible_devices: Optional[List[int]] = None,
        requires_memory_per_device: int = 0,
        min_devices: int = 1,
    ) -> None:
        """
        Initialize a ProcTask with a shell command.

        Args:
            cmd (str): A magic string representing the command to execute.
            env (Optional[Dict[str, Any]], optional): Environment variables for the task.
            task_name (Optional[str], optional): Name of the task.
            priority (int, optional): Task priority.
            visible_devices (Optional[List[int]], optional): List of visible device indices.
            requires_memory_per_device (int, optional): Memory required per device in MiB.
            min_devices (int, optional): Minimum number of devices required.
        """
        ...

    @overload
    def __init__(
        self,
        func: Callable[[], None],
        env: Optional[Dict[str, Any]] = None,
        task_name: Optional[str] = None,
        priority: int = 1,
        visible_devices: Optional[List[int]] = None,
        requires_memory_per_device: int = 0,
        min_devices: int = 1,
    ) -> None:
        """
        Initialize a ProcTask with a Python callable.

        Args:
            func (Callable): A callable without any params to be executed in a new process.
            env (Optional[Dict[str, Any]], optional): Environment variables for the task.
            task_name (Optional[str], optional): Name of the task.
            priority (int, optional): Task priority.
            visible_devices (Optional[List[int]], optional): List of visible device indices.
            requires_memory_per_device (int, optional): Memory required per device in MiB.
            min_devices (int, optional): Minimum number of devices required.
        """
        ...

    def __init__(  # type: ignore
        self,
        cmd_or_func: Union[str, Callable],
        env: Optional[Dict[str, Any]] = None,
        task_name: Optional[str] = None,
        priority: int = 1,
        visible_devices: Optional[List[int]] = None,
        requires_memory_per_device: int = 0,
        min_devices: int = 1,
    ) -> None:
        super().__init__(
            task_name=task_name,
            priority=priority,
            visible_devices=visible_devices,
            requires_memory_per_device=requires_memory_per_device,
            min_devices=min_devices,
        )
        self._proc: Optional[Union[subprocess.Popen, multiprocessing.Process]] = None
        self._is_pending = False

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

    @property
    def user(self) -> str:
        """Get the user of the task."""
        return psutil.Process(self.pid).username()

    @property
    def status(self) -> TaskStatus:
        """
        Get the current status of the task.

        Returns:
            TaskStatus: Pending if not started; Running if in progress; Done/Failed based on process exit code.
        """
        if self._is_pending:
            return TaskStatus.Suspended
        if self._proc is None:
            return TaskStatus.Waiting
        if isinstance(self._proc, subprocess.Popen):
            if self._proc.poll() is None:
                return TaskStatus.Running
            return TaskStatus.Done if self._proc.returncode == 0 else TaskStatus.Failed
        else:
            if self._proc.exitcode is None:
                return TaskStatus.Running
            return TaskStatus.Done if self._proc.exitcode == 0 else TaskStatus.Failed

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
        """
        Start executing the task.
        """
        if not self.is_ready:
            raise RuntimeError(f"{self.name} is not ready to be executed.")

        is_cmd_task = hasattr(self, "cmd")
        task_env = self.env or os.environ
        merged_env = {
            **task_env,
            **self.visible_device_environ,
        }

        self.cache_dir = get_cache_dir(self)
        self._stdout = open_redirected_stream(self.cache_dir, "stdout")
        self._stderr = open_redirected_stream(self.cache_dir, "stderr")
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
        self._is_pending = False

    def wait(self, timeout: Optional[float] = None) -> None:
        """
        Block until the task finishes execution.
        """
        if self.status == TaskStatus.Running:
            if isinstance(self._proc, subprocess.Popen):
                self._proc.wait(timeout)
            elif isinstance(self._proc, multiprocessing.Process):
                self._proc.join(timeout)

    def suspend(self) -> None:
        """
        Terminate the task and reset its process to pending.
        """
        if self.status in [TaskStatus.Done, TaskStatus.Failed]:
            return
        self._is_pending = True
        if self.status == TaskStatus.Running:
            self.terminate(force=True)
            self._proc = None

    def resume(self) -> None:
        """
        Resume a pending task.
        """
        self._is_pending = False

    def terminate(self, force: bool = False) -> None:
        """
        Terminate the running task.
        """
        if self.status == TaskStatus.Running:
            self._proc.kill() if force else self._proc.terminate()  # type: ignore

    def on_end(self) -> None:
        """
        Callback executed when the task ends.
        """
        super().on_end()
        self._stdout.close()
        self._stderr.close()
