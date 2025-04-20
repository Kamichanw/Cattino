import os

TASK_GLOBALS_KEY = "__catin_tasks"

# default cache
default_home = os.path.join(os.path.expanduser("~"), ".cache")
DEFAULT_CATTINO_HOME = os.path.join(default_home, "cattino")
CATTINO_HOME = os.path.expanduser(os.getenv("CATTINO_HOME", DEFAULT_CATTINO_HOME))
CACHE_DIR_FORMAT = os.getenv(
    "CATTINO_DIR_FORMAT", os.path.join("%Y-%m-%d", "%H-%M-%S", "${fullname}")
)
# constants for the backend server
CATTINO_HOST = os.getenv("CATTINO_HOST", "localhost")
CATTINO_PORT = int(os.getenv("CATTINO_PORT", 19192))

CATTINO_RETRY_EXIT_CODE = 100
