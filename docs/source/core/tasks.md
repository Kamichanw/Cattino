# Tasks

This page describes the core task abstractions used by `cattino`: how tasks are defined, composed, exported and executed. It includes examples you can copy into a Python file to create task graphs and run them with the CLI.

## Concepts

- **Task**: A unit of work. Tasks encapsulate a command, function, or subprocess to run and attach metadata like resource requirements and priority.
- **ProcTask**: A task that runs a subprocess/command. This is the most common type when invoking training scripts or evaluation scripts.
- **TaskGroup / TaskGraph**: Containers that describe relationships between tasks. A `TaskGroup` can accept a list of tasks or a `TaskGraph` (a directed acyclic graph) to express dependencies and execution order.
- **Scheduler / Backend**: The runtime that receives exported tasks and schedules them to devices based on requirements (devices, memory, priorities).

## Creating Tasks

There are two common patterns to create tasks:

1. Create a task from a command string (convenient for quick one-off tasks).
2. Create programmatic tasks (subclassing `ProcTask` or using `TaskGroup`/`TaskGraph`) when you need complex dependencies or lifecycle hooks.

### Example: simple `ProcTask` from Python

Save this to `task.py` and then run `meow create task.py` or export it (see Exporting section).

```python
from cattino.tasks import ProcTask

task = ProcTask(
	"python -c \"print(\'hello world\')\"",
	task_name="hello",
	min_devices=1,
	requires_memory_per_device=1000,
)

if __name__ == "__main__":
	# For local testing you may want to dispatch/run the task via the API
	import cattino
	cattino.export(task)
```

### Example: TaskGroup with dependencies

This example shows a training → evaluation pipeline.

```python
from cattino.tasks import ProcTask, TaskGroup

train = ProcTask(
	"python train.py",
	task_name="train",
	min_devices=2,
	requires_memory_per_device=20000,
)

eval = ProcTask(
	"python eval.py",
	task_name="eval",
	min_devices=1,
	requires_memory_per_device=20000,
)

group = TaskGroup([train, eval], execute_strategy="sequential", group_name="pipeline")

import cattino
cattino.export(group)
```

In a `TaskGroup` with `execute_strategy="sequential"`, tasks run in the order provided. To express arbitrary DAG dependencies, construct a `TaskGraph` and pass it to `TaskGroup` instead.

## Task Graphs (DAGs)

Use `TaskGraph` when you need fine-grained dependency control. You can add tasks and edges between them to form a DAG. The scheduler will only run tasks whose prerequisites are satisfied.

```python
from cattino.tasks import ProcTask, TaskGraph, TaskGroup

t1 = ProcTask("python step1.py", task_name="step1")
t2 = ProcTask("python step2.py", task_name="step2")
t3 = ProcTask("python step3.py", task_name="step3")

g = TaskGraph()
g.add_tasks_from([t1, t2, t3])
g.add_edges_from([(t1, t2), (t2, t3)])

group = TaskGroup(tasks=t1, execute_strategy=g)
import cattino
cattino.export(group)
```

## Lifecycle hooks and subclassing

If you need to run Python code before or after task execution, subclass `ProcTask` and override lifecycle hooks like `on_start` and `on_end`.

```python
from cattino.tasks import ProcTask

class MyTask(ProcTask):
	async def on_start(self):
		await super().on_start()
		print(f"Task {self.name} starting")

	async def on_end(self):
		await super().on_end()
		print(f"Task {self.name} finished")
```

## Exporting tasks

To send the task object to the cattino backend (so the CLI/server can schedule it), call `cattino.export(task_or_group)` from a script. The `meow create script.py` CLI will import the script and the exported object will be registered and sent to the backend.

Example:

```python
import cattino
from cattino.tasks import ProcTask

task = ProcTask("python train.py", task_name="train")
cattino.export(task)
```

Then run from shell:

```
meow create task.py
```

or to run a quick local interactive server use:

```
meow run
```