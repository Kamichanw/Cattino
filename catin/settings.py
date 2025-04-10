import os
from typing import Any, Mapping
import toml  # type: ignore
from catin.utils import get_catin_home


class Settings(Mapping):
    """
    Global settings for catin. To add new settings, add them to the `default_setting` property and
    create a property for them. The property should have a docstring that describes the setting.

    Once a setting is added, it can be set with `meow set <setting> <value>`. All underlines in names
    are replaced with dashes. For example, `override_exist_tasks` becomes `override-exist-tasks`.
    The value type of settings can be any types that can be serialized to toml.
    """

    def __init__(self):
        self.load()

    def load(self):
        self._settings = {}
        if os.path.isfile(self.path):
            with open(self.path, "r") as f:
                config = toml.load(f)
            self._settings.update(config.get("tool", {}).get("catin", {}))

    def save(self):
        # only save those that are different from the default settings
        new_settings = {
            k: self._settings[k]
            for k in self.default_settings.keys()
            if k in self._settings and self._settings[k] != self.default_settings[k]
        }

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

        with open(self.path, "w") as f:
            toml.dump(config, f)

    def clear(self):
        """Clear all settings."""
        self._settings = {}
        self.save()

    @property
    def default_settings(self):
        return {
            "override_exist_tasks": False,
            "cascade_cancel_on_failure": False,
            "debugging": False,
        }

    @property
    def all_settings(self):
        return {**self.default_settings, **self._settings}

    @property
    def path(self):
        search_path = [
            os.path.join(os.getcwd(), "pyproject.toml"),
            os.path.join(get_catin_home(), "settings"),
        ]
        for path in search_path:
            if os.path.isfile(path):
                return path
        return None

    def set(self, key: str, value: Any):
        """
        Set a setting to a new value.

        Args:
            key (str): The name of the setting to update. It must be a key of `settings.default_settings`.
            value (str): The value to set for the setting. This can be any type that can be serialized to toml.
        """
        self.load()
        self._settings[key] = value
        self.save()

    def get(self, key: str, default: Any = None):
        """
        Get the value of a setting.

        Args:
            key (str): The name of the setting to get. It must be a key of `settings.default_settings`.
        """
        self.load()
        return self._settings.get(key, default)

    @property
    def override_exist_tasks(self) -> bool:
        """Whether to override existing tasks. Defaults to False."""
        return self.get("override_exist_tasks", False)

    @override_exist_tasks.setter
    def override_exist_tasks(self, value: bool):
        if not isinstance(value, bool):
            raise ValueError(
                f"override_exist_tasks must be a boolean value, but got {type(value).__name__}"
            )
        self.set("override_exist_tasks", value)

    @property
    def cascade_cancel_on_failure(self) -> bool:
        """Whether to cancel all subsequent tasks if one fails. Defaults to False."""
        return self.get("cascade_cancel_on_failure", False)

    @cascade_cancel_on_failure.setter
    def cascade_cancel_on_failure(self, value: bool):
        if not isinstance(value, bool):
            raise ValueError(
                f"cascade_cancel_on_failure must be a boolean value, but got {type(value).__name__}"
            )
        self.set("cascade_cancel_on_failure", value)

    @property
    def debugging(self) -> bool:
        """Whether to enable debugging mode. Defaults to False."""
        return self.get("debugging", False)

    @debugging.setter
    def debugging(self, value: bool):
        if not isinstance(value, bool):
            raise ValueError(
                f"debugging must be a boolean value, but got {type(value).__name__}"
            )
        self.set("debugging", value)

    def __getitem__(self, key):
        return getattr(self, key)

    def __iter__(self):
        return iter(self.default_settings)

    def __len__(self):
        return len(self.default_settings)

    def __repr__(self):
        return f"Settings({self.all_settings})"


settings = Settings()
