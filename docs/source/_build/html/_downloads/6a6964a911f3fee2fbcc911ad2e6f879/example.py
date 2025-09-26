import argparse
import asyncio
import random
import time

from tqdm import tqdm
import cattino
from cattino.tasks import TaskGroup, TaskGraph, ProcTask


class MyTask(ProcTask):

    async def on_start(self):
        await super().on_start()
        print(f"Task {self.name} started")

    async def on_end(self):
        # make sure call super() first
        await super().on_end()
        print(f"Task {self.name} ended")


class MyTaskGroup(TaskGroup):
    def __init__(self, tasks, execute_strategy="sequential"):
        super().__init__(tasks=tasks, execute_strategy=execute_strategy)

    async def on_start(self):
        await super().on_start()
        print("Task group started")

    async def on_end(self):
        await super().on_end()
        print("Task group ended")


def progress_bar_task():
    print("Task started")
    for i in tqdm(range(10), desc="Processing", unit="task"):
        time.sleep(1)
        print(i)


parser = argparse.ArgumentParser()
parser.add_argument("-n", "--num", type=int, default=10, help="Number of tasks to run")

args = parser.parse_args()
num = args.num

l = [random.randint(10, 20) for i in range(num)]

tasks = [
    MyTask(
        f"python -c \"import time; print('{i}'); time.sleep({l[i]})\"",
        task_name=f"{num}task-{i}",
        requires_memory_per_device=20000,
    )
    for i in range(num)
]

g = TaskGraph()
g.add_tasks_from(tasks)
g.add_edges_from([(tasks[0], tasks[i]) for i in range(1, len(tasks) - 1)])
g.add_edges_from([(tasks[i], tasks[-1]) for i in range(len(tasks) - 1)])
tasks_group = MyTaskGroup(tasks=tasks, execute_strategy=g)

cattino.export(tasks_group)
# cattino.export(MyTask(progress_bar_task, task_name=f"{num}process-bar"))

# from cattino.core.task_scheduler import TaskScheduler
# scheduler = TaskScheduler()
# async def main():
#     await scheduler.dispatch(tasks_group)
#     result = await scheduler.step()
#     print(result)


# asyncio.run(main())
