# SPDX-License-Identifier: Apache-2.0
"""Code inside this file can safely assume cuda platform, e.g. importing
pynvml. However, it should not initialize cuda context.
"""

import os
from functools import wraps
import psutil
from typing import Callable, List, Optional, TypeVar, Union

import torch
from typing_extensions import ParamSpec

from .interface import Platform, PlatformEnum
from cattino.utils import import_pynvml

pynvml = import_pynvml()


def device_id_to_physical_device_id(device_id: int) -> int:
    if "CUDA_VISIBLE_DEVICES" in os.environ:
        device_ids = os.environ["CUDA_VISIBLE_DEVICES"].split(",")
        if device_ids == [""]:
            raise RuntimeError(
                "CUDA_VISIBLE_DEVICES is set to empty string, which means"
                " GPU support is disabled."
            )
        physical_device_id = device_ids[device_id]
        return int(physical_device_id)
    else:
        return device_id


_P = ParamSpec("_P")
_R = TypeVar("_R")


def with_nvml_context(fn: Callable[_P, _R]) -> Callable[_P, _R]:

    @wraps(fn)
    def wrapper(*args: _P.args, **kwargs: _P.kwargs) -> _R:
        pynvml.nvmlInit()
        try:
            return fn(*args, **kwargs)
        finally:
            pynvml.nvmlShutdown()

    return wrapper


class CudaPlatform(Platform):
    _enum = PlatformEnum.CUDA
    device_name: str = "cuda"
    device_type: str = "cuda"

    @classmethod
    def get_device_control_env_var(cls, device_ids):
        return {
            "CUDA_VISIBLE_DEVICES": ",".join(
                str(device_id_to_physical_device_id(idx)) for idx in device_ids
            )
        }

    @classmethod
    def get_all_deivce_indeces(cls) -> List[int]:
        return list(range(torch.cuda.device_count()))

    @classmethod
    @with_nvml_context
    def get_device_name(cls, device_id: int = 0) -> str:
        physical_device_id = device_id_to_physical_device_id(device_id)
        handle = pynvml.nvmlDeviceGetHandleByIndex(physical_device_id)
        return pynvml.nvmlDeviceGetName(handle)

    @classmethod
    @with_nvml_context
    def get_device_uuid(cls, device_id: int = 0) -> str:
        physical_device_id = device_id_to_physical_device_id(device_id)
        handle = pynvml.nvmlDeviceGetHandleByIndex(physical_device_id)
        return pynvml.nvmlDeviceGetUUID(handle)

    @classmethod
    @with_nvml_context
    def get_device_total_memory(cls, device_id: int = 0) -> int:
        physical_device_id = device_id_to_physical_device_id(device_id)
        handle = pynvml.nvmlDeviceGetHandleByIndex(physical_device_id)
        return pynvml.nvmlDeviceGetMemoryInfo(handle).total // 1024**2

    @classmethod
    @with_nvml_context
    def get_device_free_memory(cls, device_id: int = 0) -> int:
        physical_device_id = device_id_to_physical_device_id(device_id)
        handle = pynvml.nvmlDeviceGetHandleByIndex(physical_device_id)
        return pynvml.nvmlDeviceGetMemoryInfo(handle).free // 1024**2

    @classmethod
    @with_nvml_context
    def get_proc_memory_usage(
        cls,
        pid_or_proc: Optional[Union[int, psutil.Process]] = None,
        device_id: int = 0,
        include_children: Optional[bool] = None,
    ):
        if pid_or_proc is None:
            target_pids = None
        else:
            if isinstance(pid_or_proc, int):
                proc = psutil.Process(pid_or_proc)
            elif isinstance(pid_or_proc, psutil.Process):
                proc = pid_or_proc
            else:
                raise ValueError(
                    f"pid_or_proc must be int or psutil.Process, got {type(pid_or_proc)}"
                )

            target_pids = [proc.pid]
            if include_children:
                target_pids.extend(child.pid for child in proc.children(recursive=True))

        physical_device_id = device_id_to_physical_device_id(device_id)
        handle = pynvml.nvmlDeviceGetHandleByIndex(physical_device_id)
        procs = pynvml.nvmlDeviceGetComputeRunningProcesses(handle)

        if target_pids is None:
            return sum(p.usedGpuMemory for p in procs) // (1024**2)
        else:
            return sum(p.usedGpuMemory for p in procs if p.pid in target_pids) // (
                1024**2
            )
