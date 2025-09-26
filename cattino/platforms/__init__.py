# SPDX-License-Identifier: Apache-2.0
# Modified from vLLM's codebase.
import traceback
from typing import TYPE_CHECKING, Optional

from cattino.utils import resolve_obj_by_qualname

from .interface import Platform, PlatformEnum


def tpu_platform_plugin() -> Optional[str]:
    is_tpu = False
    try:
        # While it's technically possible to install libtpu on a
        # non-TPU machine, this is a very uncommon scenario. Therefore,
        # we assume that libtpu is installed if and only if the machine
        # has TPUs.
        import libtpu  # type: ignore

        is_tpu = True
    except Exception as e:
        pass

    return "cattino.platforms.tpu.TpuPlatform" if is_tpu else None


def cuda_platform_plugin() -> Optional[str]:
    is_cuda = False
    try:
        from cattino.utils import import_pynvml

        pynvml = import_pynvml()
        pynvml.nvmlInit()
        try:
            is_cuda = pynvml.nvmlDeviceGetCount() > 0
        finally:
            pynvml.nvmlShutdown()
    except Exception as e:
        if "nvml" not in e.__class__.__name__.lower():
            # If the error is not related to NVML, re-raise it.
            raise e

        import os

        def cuda_is_jetson() -> bool:
            return os.path.isfile("/etc/nv_tegra_release") or os.path.exists(
                "/sys/class/tegra-firmware"
            )

        if cuda_is_jetson():
            raise RuntimeError("cattino doesn't support jetson platform.")

    return "cattino.platforms.cuda.CudaPlatform" if is_cuda else None


def rocm_platform_plugin() -> Optional[str]:
    is_rocm = False
    try:
        import amdsmi  # type: ignore

        amdsmi.amdsmi_init()
        try:
            if len(amdsmi.amdsmi_get_processor_handles()) > 0:
                is_rocm = True
        finally:
            amdsmi.amdsmi_shut_down()
    except Exception as e:
        pass

    return "cattino.platforms.rocm.RocmPlatform" if is_rocm else None


def hpu_platform_plugin() -> Optional[str]:
    is_hpu = False
    try:
        from importlib import util

        is_hpu = util.find_spec("habana_frameworks") is not None

    except Exception as e:
        pass

    return "cattino.platforms.hpu.HpuPlatform" if is_hpu else None


def xpu_platform_plugin() -> Optional[str]:
    is_xpu = False
    try:
        # installed IPEX if the machine has XPUs.
        import intel_extension_for_pytorch  # type: ignore
        import oneccl_bindings_for_pytorch  # type: ignore
        import torch

        if hasattr(torch, "xpu") and torch.xpu.is_available():
            is_xpu = True
    except Exception as e:
        pass

    return "cattino.platforms.xpu.XPUPlatform" if is_xpu else None


def cpu_platform_plugin() -> Optional[str]:
    return "cattino.platforms.cpu.CpuPlatform"


def neuron_platform_plugin() -> Optional[str]:
    is_neuron = False
    try:
        import transformers_neuronx  # type: ignore

        is_neuron = True
    except ImportError as e:
        pass

    return "cattino.platforms.neuron.NeuronPlatform" if is_neuron else None


def ascend_platform_plugin() -> Optional[str]:
    is_ascend = False
    try:
        import torch

        is_ascend = torch.npu.is_available()  # type: ignore
    except:
        pass

    return "cattino.platforms.ascend.AscendPlatform" if is_ascend else None


builtin_platform_plugins = {
    "tpu": tpu_platform_plugin,
    "cuda": cuda_platform_plugin,
    "rocm": rocm_platform_plugin,
    "hpu": hpu_platform_plugin,
    "xpu": xpu_platform_plugin,
    "cpu": cpu_platform_plugin,
    "neuron": neuron_platform_plugin,
    "ascend": ascend_platform_plugin,
}


def resolve_current_platform_cls_qualname() -> str:
    activated_plugins = []

    for name, func in builtin_platform_plugins.items():
        if func() is not None:
            activated_plugins.append(name)

    while len(activated_plugins) > 1:
        if "cpu" in activated_plugins:
            activated_plugins.remove("cpu")
        else:
            raise RuntimeError(
                "Only one platform plugin can be activated, but got: "
                f"{activated_plugins}"
            )

    if len(activated_plugins) == 1:
        platform_cls_qualname = builtin_platform_plugins[activated_plugins[0]]()
    else:
        raise RuntimeError("No available platform is found.")
    return platform_cls_qualname  # type: ignore


_current_platform: Optional[Platform] = None
_init_trace: str = ""

if TYPE_CHECKING:
    current_platform: Platform


def __getattr__(name: str):
    if name == "current_platform":
        # lazy init current_platform.
        # 1. out-of-tree platform plugins need `from cattino.platforms import
        #    Platform` so that they can inherit `Platform` class. Therefore,
        #    we cannot resolve `current_platform` during the import of
        #    `cattino.platforms`.
        # 2. when users use out-of-tree platform plugins, they might run
        #    `import cattino`, some cattino internal code might access
        #    `current_platform` during the import, and we need to make sure
        #    `current_platform` is only resolved after the plugins are loaded
        #    (we have tests for this, if any developer violate this, they will
        #    see the test failures).
        global _current_platform
        if _current_platform is None:
            platform_cls_qualname = resolve_current_platform_cls_qualname()
            _current_platform = resolve_obj_by_qualname(platform_cls_qualname)()
            global _init_trace
            _init_trace = "".join(traceback.format_stack())
        return _current_platform
    elif name in globals():
        return globals()[name]
    else:
        raise AttributeError(f"No attribute named '{name}' exists in {__name__}.")


__all__ = [
    "Platform",
    "PlatformEnum",
    "current_platform",
    "_init_trace",
]
