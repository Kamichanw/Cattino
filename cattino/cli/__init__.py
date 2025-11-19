from importlib import import_module
import pkgutil
from pathlib import Path

# iterate modules in this package directory and import them
package_dir = Path(__file__).parent
for finder, name, ispkg in pkgutil.iter_modules([str(package_dir)]):
    # only import command modules that explicitly end with `_cmd`
    if not name.endswith("_cmd"):
        continue
    import_module(f".{name}", package=__package__)
