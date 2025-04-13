from typing import Any, Dict, List, Optional, Union
import uuid
from psutil import Process
import psutil

from .interface import Platform, PlatformEnum


class CpuPlatform(Platform):
    _enum = PlatformEnum.CPU
    device_name: str = "cpu"
    device_type: str = "cpu"

    @classmethod
    def get_device_control_env_var(
        cls, device_ids: List[int]
    ) -> Dict[str, Any]:
        """
        Returns the environment variables to control device visibility.
        For CPU, no device control is necessary.
        """
        return {}

    @classmethod
    def get_all_deivce_indeces(cls) -> List[int]:
        """
        Returns the number of available CPU devices.
        For CPU, only one device exists.
        """
        return [0]

    @classmethod
    def get_device_name(cls, device_id: int = 0) -> str:
        """
        Returns the device name.
        For CPU, returns a generic 'CPU' string.
        """
        return "CPU"

    @classmethod
    def get_device_uuid(cls, device_id: int = 0) -> str:
        """
        Returns a unique identifier for the device.
        For CPU, generates a UUID based on the machine's MAC address.
        """
        unique_id = uuid.UUID(int=uuid.getnode())
        return str(unique_id)

    @classmethod
    def get_device_total_memory(cls, device_id: int = 0) -> int:
        """
        Returns the total memory of the system in MB.
        For CPU, returns the total system RAM.
        """
        vm = psutil.virtual_memory()
        return vm.total // (1024**2)
    
    @classmethod
    def get_device_free_memory(cls, device_id: int = 0) -> int:
        """
        Returns the free memory of the system in MB.
        For CPU, returns the free system RAM.
        """
        vm = psutil.virtual_memory()
        return vm.free // (1024**2)

    @classmethod
    def get_proc_memory_usage(
        cls,
        pid_or_proc: Optional[Union[int, Process]] = None,
        device_id: int = 0,
        include_children: Optional[bool] = False,
    ) -> int:
        """
        Queries the memory usage of a specific process or all process in the system's RAM (in MB).

        Args:
            pid_or_proc (int or psutil.Process, optional):
                The process ID or a Process object to query.
                If None, returns memory usage for all processes.
            device_id (int, optional):
                Not applicable in a CPU environment; ignored.
            include_children (bool, optional):
                If True, includes the memory usage of child processes. Defaults to False.

        Returns:
            Union[int, list]:
                If a specific process is queried, returns the memory used by that process (and its children, if applicable) in MB.
                If no process is specified, returns a list of all processes with their respective memory usage details.
        """
        if pid_or_proc is None:
            vm = psutil.virtual_memory()
            return vm.free // (1024**2)
        if isinstance(pid_or_proc, int):
            proc = Process(pid_or_proc)
        elif isinstance(pid_or_proc, Process):
            proc = pid_or_proc
        else:
            raise ValueError(
                f"pid_or_proc must be int or psutil.Process, got {type(pid_or_proc)}"
            )

        proc_mem = proc.memory_info().rss
        if include_children:
            for child in proc.children(recursive=True):
                try:
                    proc_mem += child.memory_info().rss
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    continue
        return proc_mem // (1024**2)
