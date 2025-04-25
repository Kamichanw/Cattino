import os
from typing import TYPE_CHECKING, Dict, List, Optional, Set

from cattino.settings import settings
from cattino.platforms import current_platform

if TYPE_CHECKING:
    from cattino.tasks.interface import DeviceRequiredTask


class DeviceAllocator:
    """
    DeviceAllocator for managing device resource allocation.

    This class loads the device utility module, queries device memory,
    and tracks running tasks to determine available devices. A singleton
    instance is maintained.
    """

    _instance: Optional["DeviceAllocator"] = None
    _running_tasks: Set["DeviceRequiredTask"] = set()

    def __new__(cls) -> "DeviceAllocator":
        """
        Create and initialize a new instance of DeviceAllocator if one does not already exist.
        This implements the singleton pattern with thread safety.
        """
        if cls._instance is None:
            instance = super().__new__(cls)
            cls._instance = instance
        return cls._instance

    def allocate(self, task: "DeviceRequiredTask") -> None:
        """
        Allocate devices for the given task based on its resource requirements.
        If sufficient devices are available, update the task's `assigned_device_indices`
        and add the task to the running tasks list.
        Otherwise, leave the task's allocated devices unset.

        Args:
            task (DeviceRequiredTask): The task instance requiring device allocation.
        """
        if task.assigned_device_indices is not None:
            return
        if task.requires_memory_per_device == 0 or task.min_devices == 0:
            task._assigned_device_indices = []
            return

        if task.min_devices > len(settings.visible_devices):
            raise ValueError(
                f"Not enough devices: required {task.min_devices} but only {len(settings.visible_devices)} visible."
            )

        max_memory = max(
            current_platform.get_device_total_memory(idx)
            for idx in settings.visible_devices
        )
        if task.requires_memory_per_device > max_memory:
            raise ValueError(
                f"Insufficient memory: requires {task.requires_memory_per_device} MiB, but max available is {max_memory} MiB."
            )

        # get free memory that is not controlled by cattino
        free_memory = {
            device_id: current_platform.get_device_free_memory(device_id)
            + current_platform.get_proc_memory_usage(
                pid_or_proc=os.getpid(), device_id=device_id, include_children=True
            )
            for device_id in settings.visible_devices
        }

        for running_task in DeviceAllocator._running_tasks:
            for device_id in running_task.assigned_device_indices or []:
                free_memory[device_id] -= running_task.requires_memory_per_device

        avail_devices = [
            idx
            for idx, mem in free_memory.items()
            if mem >= task.requires_memory_per_device
        ]

        if len(avail_devices) >= task.min_devices:
            task._assigned_device_indices = avail_devices[: task.min_devices]
            DeviceAllocator._running_tasks.add(task)

    def release(self, task: "DeviceRequiredTask") -> None:
        """
        Release the given task from the running tasks list.

        Args:
            task (DeviceRequiredTask): The task to release.
        """
        if task in DeviceAllocator._running_tasks:
            DeviceAllocator._running_tasks.remove(task)

    @classmethod
    def get_device_control_env_var(
        cls, assigned_device_indices: List[int]
    ) -> Dict[str, str]:
        """
        Get environment variables for device visibility.

        Args:
            assigned_device_indices (List[int]): The allocated device indices.

        Returns:
            Dict[str, str]: Environment variables for device visibility.
        """
        return current_platform.get_device_control_env_var(assigned_device_indices)

    @classmethod
    def get_all_device_indices(cls) -> List[int]:
        """
        Get all device indices. This will not affected by the device visibility.
        """
        return current_platform.get_all_deivce_indeces()
