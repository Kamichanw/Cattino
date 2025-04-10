<p align="center">
  <img src="./assets/illustration.png" alt="main" style="width: 50%;"/>
</p>


**Catin** is a command-line tool designed for managing tasks with complex dependencies. It is highly configurable, yet comes with sensible defaults out of the box. It is commonly used in AI model training and evaluation pipelines. In this context, it can automatically detect available devices, schedule tasks according to plan, and save execution logs automatically.

# Installation
In the current version, only installation from source is supported.
```bash
git clone https://github.com/Kamichanw/Catin.git
cd ./Catin
pip install -e .
```
Once the installation is complete, you can use `meow meow` in the command line terminal. If the installation is successful, it will output the current version number.

# Quickstart
If you already have your own training script `train.py`, which requires 2 devices with 20000M memory each, you can dispatch the task using the following command:

```shell
meow create "python train.py" --task-name train --min-devices 2 --requires-memory-per-device 20000
```

When creating the task for the first time, Catin will automatically start a local server. If sufficient devices are available, the task will start immediately. Otherwise, it will wait until the conditions are met.

Once the task starts running, its output will be automatically redirected to a cached log file. You can view the task's output by running `meow watch <your-task-name>`.

Catin can seamlessly integrate with `hydra` or `argparse`. Assuming your script accepts additional options, you can use `--` at the end of the command and add the extra options afterward:

```shell
# use with argparse
# equals to python train.py --lr 0.2
meow create "python train.py" -- --lr 0.2
```

> [!TIP]
> You can also set dependencies between tasks, ensuring that certain tasks must execute after others. Alternatively, some tasks can run in parallel (if possible). 
> For more details, refer to the [basic usage](./docs/basic_usages.md).
