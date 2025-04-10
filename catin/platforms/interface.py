# Modified from vLLM's codebase to catin's codebase.
import enum
from typing import Dict, List, Optional, Union

from psutil import Process


class PlatformEnum(enum.Enum):
    CUDA = enum.auto()
    ROCM = enum.auto()
    TPU = enum.auto()
    HPU = enum.auto()
    XPU = enum.auto()
    CPU = enum.auto()
    ASCEND = enum.auto()
    NEURON = enum.auto()
    OOT = enum.auto()
    UNSPECIFIED = enum.auto()


class Platform:
    _enum: PlatformEnum
    device_name: str
    device_type: str

    def is_cuda(self) -> bool:
        return self._enum == PlatformEnum.CUDA

    def is_rocm(self) -> bool:
        return self._enum == PlatformEnum.ROCM

    def is_tpu(self) -> bool:
        return self._enum == PlatformEnum.TPU

    def is_hpu(self) -> bool:
        return self._enum == PlatformEnum.HPU

    def is_xpu(self) -> bool:
        return self._enum == PlatformEnum.XPU

    def is_cpu(self) -> bool:
        return self._enum == PlatformEnum.CPU

    def is_neuron(self) -> bool:
        return self._enum == PlatformEnum.NEURON

    def is_ascend(self) -> bool:
        return self._enum == PlatformEnum.ASCEND

    def is_out_of_tree(self) -> bool:
        return self._enum == PlatformEnum.OOT

    def is_cuda_alike(self) -> bool:
        """Stateless version of :func:`torch.cuda.is_available`."""
        return self._enum in (PlatformEnum.CUDA, PlatformEnum.ROCM)

    @classmethod
    def get_all_deivce_indeces(cls) -> List[int]:
        """Get all logic indices of available devices"""
        raise NotImplementedError

    @classmethod
    def get_device_control_env_var(cls, device_ids: List[int]) -> Dict[str, str]:
        """
        Get a dictionary to set device control environment variable.
        For example, for GPU, this method should return something like
        `{ "CUDA_VISIBLE_DEVICES": [0, 1, 2] }`. Note that this method
        should convert the device IDs to the physical device IDs.
        """
        raise NotImplementedError

    @classmethod
    def get_device_name(cls, device_id: int = 0) -> str:
        """Get the name of a device."""
        raise NotImplementedError

    @classmethod
    def get_device_uuid(cls, device_id: int = 0) -> str:
        """Get the uuid of a device, e.g. the PCI bus ID."""
        raise NotImplementedError

    @classmethod
    def get_device_total_memory(cls, device_id: int = 0) -> int:
        """Get the total memory of a device in MiB."""
        raise NotImplementedError

    @classmethod
    def get_device_free_memory(cls, device_id: int = 0) -> int:
        """Get the free memory of a device in MiB."""
        raise NotImplementedError

    @classmethod
    def get_proc_memory_usage(
        cls,
        pid_or_proc: Optional[Union[int, Process]] = None,
        device_id: int = 0,
        include_children: Optional[bool] = None,
    ) -> int:
        """
        Query the device memory usage of a specific process or all process. Optionally, include the memory usage of the process's children.

        Args:
            pid_or_proc (int or psutil.Process, *optional*):
                The process ID (PID) or a Process object to query. If None, queries the memory usage for all
                process. Defaults to None.
            device_id (int):
                The device index to filter by. Defaults to 0.
            include_children (bool, *optional*):
                If True, includes the memory usage of child processes. Defaults to False.
        """
        raise NotImplementedError


class UnspecifiedPlatform(Platform):
    _enum = PlatformEnum.UNSPECIFIED
    device_type = ""
