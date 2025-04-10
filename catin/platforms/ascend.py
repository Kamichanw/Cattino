import os
import subprocess
from collections import namedtuple
from typing import Any, Dict, List, Optional, Union

from psutil import Process
import torch
from .interface import Platform, PlatformEnum


def device_id_to_physical_device_id(device_id: int) -> int:
    if "ASCEND_VISIBLE_DEVICES" in os.environ:
        device_ids = os.environ["ASCEND_VISIBLE_DEVICES"].split(",")
        if device_ids == [""]:
            msg = (
                "ASCEND_VISIBLE_DEVICES is set to empty string, which means "
                "Ascend NPU support is disabled."
            )
            raise RuntimeError(msg)
        physical_device_id = device_ids[device_id]
        return int(physical_device_id)
    else:
        return device_id


class AscendPlatform(Platform):
    _enum = PlatformEnum.ASCEND
    device_name: str = "ascend"
    device_type: str = "ascend"

    @classmethod
    def get_device_control_env_var(
        cls, physical_device_ids: List[int]
    ) -> Dict[str, Any]:
        return {
            "ASCEND_VISIBLE_DEVICES": ",".join(str(idx) for idx in physical_device_ids)
        }

    @classmethod
    def get_all_deivce_indeces(cls) -> List[int]:
        raise NotImplementedError

    @classmethod
    def get_device_name(cls, device_id: int = 0) -> str:
        return torch.npu.get_device_name(device_id)  # type: ignore
    
    @classmethod
    def get_device_free_memory(cls, device_id: int = 0) -> int:
        return 0

    @classmethod
    def get_device_uuid(cls, device_id: int = 0) -> str:
        physical_device_id = device_id_to_physical_device_id(device_id)
        proc = subprocess.run(
            ["npu-smi", "info", "-u"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        proc.check_returncode()
        uuids = [
            line.split(":")[1].strip()
            for line in proc.stdout.strip().splitlines()
            if "UUID" in line
        ]
        return uuids[physical_device_id]

    @classmethod
    def get_device_total_memory(cls, device_id: int = 0) -> int:
        return torch.npu.get_device_properties(device_id).total_memory // (1024**2)  # type: ignore

    @classmethod
    def get_proc_used_memory(
        cls,
        pid_or_proc: Optional[Union[int, Process]] = None,
        device_id: int = 0,
        include_children: Optional[bool] = False,
    ) -> int:
        device_id = device_id_to_physical_device_id(device_id)

        proc = subprocess.run(
            ["npu-smi", "info", "-p"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        proc.check_returncode()

        ProcMemoryUsage = namedtuple("ProcMemoryUsage", ["pid", "used_memory"])
        all_proc_used_memory = []
        for line in proc.stdout.strip().splitlines():
            parts = line.split(",")
            if len(parts) != 3:
                continue
            pid_str, dev_id_str, used_memory_str = parts
            if int(dev_id_str.strip()) == device_id:
                all_proc_used_memory.append(
                    ProcMemoryUsage(int(pid_str), int(used_memory_str))
                )

        if pid_or_proc is None:
            return sum(item.used_memory for item in all_proc_used_memory)

        process = Process(pid_or_proc) if isinstance(pid_or_proc, int) else pid_or_proc
        if not isinstance(process, Process):
            raise ValueError(
                f"pid_or_proc must be int or psutil.Process, got {type(pid_or_proc)}"
            )
        target_pids = [process.pid]
        if include_children:
            target_pids.extend(child.pid for child in process.children(recursive=True))

        return sum(
            entry.used_memory
            for entry in all_proc_used_memory
            if entry.pid in target_pids
        )
