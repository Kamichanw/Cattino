import os

TASK_GLOBALS_KEY = "__catin_tasks"

# default cache
default_home = os.path.join(os.path.expanduser("~"), ".cache")
CATIN_HOME = os.path.expanduser(
    os.getenv("CATIN_HOME", os.path.join(default_home, "catin"))
)
CACHE_DIR_FORMAT = os.getenv(
    "CATIN_DIR_FORMAT", os.path.join("%Y-%m-%d", "%H-%M-%S", "%n")
)  # %n is the name of the task
if "%n" not in CACHE_DIR_FORMAT:
    raise RuntimeError(
        "CATIN_DIR_FORMAT must contain %n, which is the name of the task"
    )

# constants for the backend server
CATIN_HOST = os.getenv("CATIN_HOST", "localhost")
CATIN_PORT = int(os.getenv("CATIN_PORT", 19192))
