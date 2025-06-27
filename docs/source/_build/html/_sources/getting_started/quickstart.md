In this document, I will demonstrate all the common usages of `cattino`. You can also directly view the help by running `meow <command> --help`.

# Introduction
When you first create a task, `cattino` will start a local server in the background. All commands are executed by sending instructions to this server. You can specify the host and port by setting the environment variables `CATTINO_HOST` and `CATTINO_PORT`.

## How Tasks to be Scheduled?
Each task has a creation time (`task.create_time`) and a priority (`task.priority`). When tasks meet preconditions, e.g. device requirements, `cattino` always runs tasks in the following order:  
1. Tasks with no dependencies or whose dependencies have all been executed.  
2. Tasks with the highest priority.  
3. Tasks with the earliest creation time.

## Logs
By default, the backend server and task outputs are automatically saved to `~/.cache/cattino/`. You can change this save path by setting the environment variable `CATTINO_HOME`. 

The specific path is related to the creation time of the backend. For example, if you open the backend on April 9, 2025, at 23:02:50, all tasks created on this backend will have their outputs saved to `{CATTINO_HOME}/2025-04-09/23-02-50/{task-name}`.

> Although you can specify the format of the save directory by modifying the environment variable `CACHE_DIR_FORMAT`, I do not recommend doing so. 
> The default format is the same as the `hydra` log directory format, making it more convenient for use with `hydra`.

Finally, you can clean cache logs with `meow clean`, use `meow clean --help` to see details.

## Run, Test and Exit
Sometimes, you may want to directly observe the backend's output, especially when exceptions thrown by tasks are written directly to the backend's stderr. In such cases, you can use `meow run`. It will start the server directly in the terminal.

To check whether the backend server is running, you can use `meow test`. This command can also be used to check if a specific task is running: `meow test {task-name}`. If it is running, you will get the process ID (PID).

To exit `cattino`, you can use `meow exit`. This will send a kill signal to all running tasks and shut down the server process.

# Create Tasks
`cattino` supports two methods for creating tasks:  
1. By using a string: `meow create "command"`.  
2. By running a Python file and exporting the task object from it: `meow create script.py`.

## Create from Command
If you only want to run individual tasks without writing extra code, creating tasks from a command is the most recommended approach. If you have a training script `train.py`, which requires 2 devices with 20000M memory each, you can dispatch the task using the following command:

```shell
meow create "python train.py" --task-name train --min-devices 2 --requires-memory-per-device 20000
```

If sufficient devices are available, the task will start immediately. Otherwise, it will wait until the conditions are met.

> [!NOTE]
> If you create a task via command, I recommend specifying a task name using `--task-name/-n` during creation. Otherwise, a 5-character name will be automatically generated, making it difficult to track the results.


## Create from Python Scripts
If you need to create complex task dependencies (for example, performing evaluation only after the training task), you must create a separate Python script to handle this. Consider that you have two scripts, `train.py` and `eval.py`, for training and evaluation respectively. You can create a `task.py`:
```python
# task.py
import cattino
from cattino.tasks import ProcTask, TaskGroup

train_task = ProcTask(
    "python train.py",
    task_name="train",
    min_devices=2,
    requires_memory_per_device=20000,
)

eval_task = ProcTask(
    "python eval.py",
    task_name="eval",
    min_devices=1,
    requires_memory_per_device=20000,
)

graph = TaskGroup(
    [train_task, eval_task], execute_strategy="sequential", group_name="pipeline"
)

# export task object with cattino.export
cattino.export(graph)
```

`ProcTask` accepts a command string or a function as input, and it will run as a subprocess when the device conditions are met.
`TaskGroup` accepts a task list or a `TaskGraph`, assembling multiple tasks and executing them in the form of a directed acyclic graph.
Once everything is set up, you also need to use `cattino.export` to export the task or task group so that `cattino` can discover the object.

## Passing Arguments
A command to be executed usually allows multiple options to perform more complex functions. Suppose `train.py` uses `argparse` to accept `--lr` to set the learning rate. You can use:

```shell
meow create "python train.py" -- --lr 0.3
# equals to `meow create "python train.py --lr 0.3"`
```

All options after `--` will be passed directly to `train.py`. When `--multirun/-m` is enabled, if the additional options are provided as a list, the command will be expanded:

```
meow create "python train.py" -m -- --lr [0.1,0.3]
# equals to 
# meow create "python train.py --lr 0.1"
# meow create "python train.py --lr 0.3"
```

> Make sure that the task names are different, otherwise an exception will be thrown due to attempting to execute duplicate tasks.


## Integrate with `hydra`
`Hydra` saves the configurations of a running application to a specific folder, which is specified by `hydra.run.dir` (see the [official documentation](https://hydra.cc/docs/tutorials/basic/running_your_app/working_directory/)). It is very convenient to save both `hydra` output files and `cattino` task outputs to the same directory. To achieve this, you can use `cattino` like this:

```shell
meow create "python train.py" -- hydra.run.dir="\${run_dir}/\${task_name}"
```

Here, `${run_dir}` and `${task_name}` are cattino's Magic variables (see [Magic String](#magic-string)), which will be replaced with the actual run directory and task name when the command is executed.

