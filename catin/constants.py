import os

TASK_GLOBALS_KEY = "__catin_tasks"

# default cache
default_home = os.path.join(os.path.expanduser("~"), ".cache")
DEFAULT_CATIN_HOME = os.path.join(default_home, "catin")
CATIN_HOME = os.path.expanduser(os.getenv("CATIN_HOME", DEFAULT_CATIN_HOME))
CACHE_DIR_FORMAT = os.getenv(
    "CATIN_DIR_FORMAT", os.path.join("%Y-%m-%d", "%H-%M-%S", "${task_name}")
)  # %n is the name of the task
if "${task_name}" not in CACHE_DIR_FORMAT:
    raise RuntimeError(
        "CATIN_DIR_FORMAT must contain ${task_name}, which is the name of the task."
    )

# constants for the backend server
CATIN_HOST = os.getenv("CATIN_HOST", "localhost")
CATIN_PORT = int(os.getenv("CATIN_PORT", 19192))
