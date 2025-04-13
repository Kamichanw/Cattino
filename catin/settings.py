import os
import re
import dill
import toml
from functools import cached_property
from typing import Any, Callable, Dict, List, Literal, Mapping, Sequence
from pydantic import BaseModel, Field, PrivateAttr

from catin.constants import CATIN_HOST, CATIN_PORT
from catin.utils import get_catin_home


class SettingsBinary(Dict[str, Any]):
    __internal_set: bool = False

    def __init__(self, bin_path: str):
        self.path = bin_path

    def load(self):
        if os.path.isfile(self.path):
            with open(self.path, "rb") as f:
                self.__internal_set = True
                self.update(dill.load(f))
                self.__internal_set = False

    def save(self):
        with open(self.path, "wb") as f:
            dill.dump(dict(self), f)

    def __setitem__(self, key: str, value: Any):
        if not self.__internal_set:
            self.load()
        super().__setitem__(key, value)
        if not self.__internal_set:
            self.save()

    def __getitem__(self, key: str) -> Any:
        self.load()
        return super().__getitem__(key)

    def clear(self):
        super().clear()
        if os.path.isfile(self.path):
            os.remove(self.path)


class Settings(BaseModel):
    """
    Global settings for catin. To add new settings, add them as a new field in this class.

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
    cascade_cancel_on_failure: bool = Field(
        False,
        description="Whether to cancel all subsequent tasks if one fails. Defaults to False.",
    )
    debugging: bool = Field(
        False, description="Whether to enable debugging mode. Defaults to False."
    )
    port: int = Field(CATIN_PORT, description="The port to use for the catin server.")
    host: str = Field(CATIN_HOST, description="The host to use for the catin server.")
    magic_vars: List[str] = Field(
        ["task_name", "run_dir"],
        description="List of magic variables to use in cli.",
    )
    resolvers: Dict[str, Callable] = Field(
        {"eval": lambda x: eval(x)},
        description="Dictionary of resolvers to use in cli.",
    )

    # this variable is used to prevent calling custom setter recursively
    __internal_set: bool = PrivateAttr(default=False)
    # this variable is used to store the binary settings
    __bin: SettingsBinary = PrivateAttr(default=None)

    class Config:
        validate_assignment = True

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.load()

    def load(self):
        if os.path.isfile(self.path):
            with open(self.path, "r") as f:
                config = toml.load(f)
            self.__internal_set = True
            for k, v in config.get("tool", {}).get("catin", {}).items():
                if isinstance(v, str):
                    m = re.match(r"^\$\{bin\.(.+)\}$", v)
                    if m:
                        # load from binary
                        k = m.group(1)
                        v = self.bin[k]
                setattr(self, k, v)
            self.__internal_set = False

    def save(self):
        # only save those that are different from the default settings
        new_settings = {
            k: self.all_settings[k]
            for k in self.default_settings.keys()
            if self.all_settings[k] != self.default_settings[k]
        }

        def is_serializable_in_toml(value) -> bool:
            """Check if a value can be directly serialized in TOML"""
            if isinstance(value, (str, int, float, bool)):
                return True
            elif isinstance(value, Sequence):
                return all(is_serializable_in_toml(v) for v in value)
            elif isinstance(value, Mapping):
                return all(is_serializable_in_toml(v) for v in value.values())
            return False

        for key, value in new_settings.items():
            if not is_serializable_in_toml(value):
                self.bin[key] = value
                new_settings[key] = f"${{bin.{key}}}"

        if os.path.isfile(self.path):
            with open(self.path, "r") as f:
                config = toml.load(f)
            config.setdefault("tool", {})
            config["tool"].setdefault("catin", {})
            config["tool"]["catin"] = new_settings
        else:
            # do not create a new file if there are no new settings
            if not new_settings:
                return
            config = {"tool": {"catin": new_settings}}

        with open(self.path, "w") as f:
            toml.dump(config, f)

    def clear(self):
        """Clear all settings."""
        self._settings = {}
        self.bin.clear()
        self.save()

    @cached_property
    def default_settings(self):
        return {name: field.default for name, field in Settings.model_fields.items()}

    @property
    def all_settings(self):
        return self.model_dump()

    @property
    def path(self) -> str:
        search_path = [
            os.path.join(os.getcwd(), "pyproject.toml"),
            os.path.join(get_catin_home(), "settings.toml"),
        ]
        for path in search_path:
            if os.path.isfile(path):
                return path
        return search_path[-1]

    @property
    def bin(self) -> Dict[str, Any]:
        """Get the binary dictionary."""
        self.__bin = SettingsBinary(os.path.join(get_catin_home(), "settings.bin"))
        return self.__bin

    def get_description(self, name: str) -> str:
        """Get the docstring of a setting."""
        return Settings.model_fields[name].description

    def get_type(self, name: str) -> type:
        """Get the type of a setting."""
        return Settings.model_fields[name].annotation

    def __getattribute__(self, name):
        if name in Settings.model_fields:
            self.load()
        return super().__getattribute__(name)

    def __setattr__(self, name, value):
        if not self.__internal_set and name in Settings.model_fields:
            self.load()
            super().__setattr__(name, value)
            self.save()
        return super().__setattr__(name, value)


settings = Settings()
