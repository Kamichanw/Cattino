import ast
import os
import re
import dill
import toml
import tempfile

from functools import cached_property
from typing import Any, Callable, Literal, Mapping, Sequence
from pydantic import BaseModel, Field, PrivateAttr
from filelock import FileLock, BaseFileLock

from cattino.constants import CATTINO_HOST, CATTINO_PORT
from cattino.platforms import current_platform
from cattino.utils import get_cattino_home


class SettingsBinary(dict[str, Any]):

    def __init__(self, bin_path: str):
        self.path = bin_path

    def __enter__(self):
        if os.path.isfile(self.path):
            with open(self.path, "rb") as f:
                self.update(dill.load(f))
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if len(self) > 0:
            dirpath = os.path.dirname(self.path)
            with tempfile.NamedTemporaryFile(dir=dirpath, delete=False) as tf:
                dill.dump(dict(self), tf)
                tmpname = tf.name
            os.replace(tmpname, self.path)

    def clear(self):
        super().clear()
        if os.path.isfile(self.path):
            try:
                os.remove(self.path)
            except Exception:
                pass


class Settings(BaseModel):
    """
    Global settings for cattino. To add new settings, add them as a new field in this class.

    Once a setting is added, it can be set with `meow set <setting> <value>`. All underlines in names
    are replaced with dashes. For example, `override_exist_tasks` becomes `override-exist-tasks`.

    The value type of settings can be any types that can be serialized by dill.
    If the value type of a setting can be directly stored in TOML, it will be saved there.
    Otherwise, the value will be stored in `settings.bin`. In TOML, the value will be represented as
    "${bin.<key>}", where `<key>` corresponds to the key in `settings.bin`.
    The `settings.bin` is stored as a `dict[str, Any]`."
    """

    override_exist_tasks: Literal["allow", "forbid", "rename"] = Field(
        "forbid",
        description=(
            "Defines how to handle existing tasks when adding new ones. "
            "'allow' will directly override existing tasks, "
            "'forbid' will raise an exception if the task exists, "
            "'rename' will add a suffix with an incremental number to the new task. "
            "Defaults to 'forbid'."
        ),
    )
    debugging: bool = Field(
        False, description="Whether to enable debugging mode. Defaults to False."
    )
    shutdown_on_complete: bool = Field(
        True,
        description=(
            "Whether to shutdown the server when all tasks are complete. Defaults to True."
        ),
    )
    visible_devices: list[int] = Field(
        current_platform.get_all_deivce_indices(),
        description=(
            "The list of visible device indices. If set to None, all devices will be visible. "
            "The indices here are logical indices, meaning they are relative to the control environment variables (e.g., CUDA_VISIBLE_DEVICES). "
            "Defaults to all devices."
        ),
    )

    port: int = Field(
        CATTINO_PORT,
        description=f"The port to use for the cattino server. Defaults to {CATTINO_PORT}.",
    )
    msgbox_port: int = Field(
        CATTINO_PORT + 1,
        description=(
            f"The port to use for the cattino message box server. Defaults to {CATTINO_PORT + 1}."
        ),
    )
    ghost_port: int = Field(
        CATTINO_PORT + 2,
        description=(
            f"The port to use for the cattino ghost server. Defaults to {CATTINO_PORT + 2}."
        ),
    )
    host: str = Field(
        CATTINO_HOST,
        description=f"The host to use for the cattino server. Defaults to {CATTINO_HOST}.",
    )

    magic_constants: dict[str, str] = Field(
        {"fullpath": "${eval:'${fullname}'.replace('/', '%s')}" % os.sep},
        description="Pre-defined constants to use in magic string.",
    )
    resolvers: dict[str, Callable] = Field(
        {"eval": lambda x: eval(x)},
        description="Resolvers to use in magic string.",
    )

    timeout: int = Field(
        5,
        gt=-1,
        description=(
            "Timeout in seconds for each command. If set to 0, all commands will wait indefinitely."
        ),
    )

    _filelock: BaseFileLock = PrivateAttr(
        default_factory=lambda: FileLock(
            os.path.join(get_cattino_home(), "settings.lock")
        )
    )
    _home: str = PrivateAttr(default_factory=get_cattino_home)
    _last_mtime: float = PrivateAttr(default=0.0)

    class Config:
        validate_assignment = True

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.load()

    def load(self):
        mtime = os.path.getmtime(self.path) if os.path.isfile(self.path) else 0.0
        if self._last_mtime >= mtime:
            return

        with open(self.path, "r") as f:
            config = toml.load(f)

        with SettingsBinary(os.path.join(get_cattino_home(), "settings.bin")) as bin:
            for k, v in config.get("tool", {}).get("cattino", {}).items():
                if isinstance(v, str):
                    # try to load from binary
                    if m := re.match(r"^\$\{bin\.(.+)\}$", v):
                        # load from binary
                        k = m.group(1)
                        v = bin.get(k)
                    object.__setattr__(self, k, v)
        self._last_mtime = os.path.getmtime(self.path)

    def save(self):
        def is_serializable_in_toml(value) -> bool:
            """Check if a value can be directly serialized in TOML"""
            if isinstance(value, (str, int, float, bool)):
                return True
            elif isinstance(value, Sequence):
                return all(is_serializable_in_toml(v) for v in value)
            elif isinstance(value, Mapping):
                return all(is_serializable_in_toml(v) for v in value.values())
            return False

        new_settings = {
            k: self.all_settings[k]
            for k in self.default_settings.keys()
            if self.all_settings[k] != self.default_settings[k]
            or is_serializable_in_toml(self.all_settings[k])
        }

        with SettingsBinary(os.path.join(get_cattino_home(), "settings.bin")) as bin:
            for key, value in new_settings.items():
                if not is_serializable_in_toml(value):
                    bin[key] = value
                    new_settings[key] = f"${{bin.{key}}}"

        dirpath = os.path.dirname(self.path) or "."
        if os.path.isfile(self.path):
            with open(self.path, "r") as f:
                config = toml.load(f)
            config.setdefault("tool", {})
            config["tool"].setdefault("cattino", {})
            config["tool"]["cattino"] = new_settings
        else:
            # do not create a new file if there are no new settings
            if not new_settings:
                return
            config = {"tool": {"cattino": new_settings}}


        with tempfile.NamedTemporaryFile(mode="w", dir=dirpath, delete=False) as tf:
            toml.dump(config, tf)
            tmpname = tf.name
        os.replace(tmpname, self.path)
        self._last_mtime = os.path.getmtime(self.path)

    def clear(self):
        """Clear all settings."""
        with self._filelock:
            if os.path.isfile(self.path):
                with SettingsBinary(
                    os.path.join(get_cattino_home(), "settings.bin")
                ) as bin:
                    bin.clear()
                try:
                    with open(self.path, "r") as f:
                        config = toml.load(f)
                except Exception:
                    config = {}
                config.setdefault("tool", {})
                config["tool"].pop("cattino", None)
                dirpath = os.path.dirname(self.path) or "."
                with tempfile.NamedTemporaryFile(
                    mode="w", dir=dirpath, delete=False
                ) as tf:
                    toml.dump(config, tf)
                    tmpname = tf.name
                os.replace(tmpname, self.path)

    @cached_property
    def default_settings(self):
        return {name: field.default for name, field in Settings.model_fields.items()}

    @property
    def all_settings(self):
        return self.model_dump()

    @property
    def path(self) -> str:
        """Get the path to the settings"""
        return os.path.join(self._home, "settings.toml")

    def get_description(self, name: str) -> str:
        """Get the docstring of a setting."""
        return Settings.model_fields[name].description or ""

    def get_type(self, name: str) -> type[Any]:
        """Get the type of a setting."""
        return Settings.model_fields[name].annotation or type[Any]

    def __getattribute__(self, name):
        """
        Load the settings from file if the attribute is a model field.
        This is to ensure that the settings are always up-to-date when accessed.
        """
        if name in Settings.model_fields:
            mtime = os.path.getmtime(self.path) if os.path.isfile(self.path) else 0.0
            if self._last_mtime < mtime:
                with self._filelock:
                    self.load()

        return super().__getattribute__(name)

    def __setattr__(self, name, value):
        """
        Set an attribute, ensuring that the settings are loaded and saved as needed.
        """
        if name in Settings.model_fields and hasattr(self, "_filelock"):
            # perform load/set/save under lock to avoid races
            with self._filelock:
                self.load()
                # use object.__setattr__ to avoid invoking this __setattr__ recursively
                object.__setattr__(self, name, value)
                try:
                    self.save()
                except Exception:
                    pass
            return

        object.__setattr__(self, name, value)


settings = Settings()
