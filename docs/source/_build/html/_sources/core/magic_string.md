# Magic String
`cattino` supports variable interpolation. Interpolations are evaluated lazily upon access. We refer to strings that support this functionality as *magic strings*. This allows you to easily use lazily initialized variables or functions in commands, simplifying the writing of `cattino` commands. For example, you can use the following command to output the log directory and task name of a new task:

```shell
meow create "echo" -- \${run_dir} \${task_name}
```

When the command is actually executing, `\${run_dir}` and `\${task_name}` will be replaced with their real values. We refer to `${variable}` as magic variables.

> In the shell, to avoid `${variable}` being interpreted as a shell variable, you need to escape it using `\$`.

You can also use functions within magic strings, which we call *resolvers*:

```shell
meow create "echo" -- \${eval: '1+2'}
# output: 3
```

The syntax is `${function: args1, args2, ...}`.

To use custom resolvers, you need to register them first. You can do this by calling `Magics.register_new_resolver` with a function that takes the arguments and returns the result. For example, if you want to create a resolver that adds two numbers:

```python
from cattino import Magics

Magics.register_new_resolver("add", lambda x, y: int(x) + int(y))
print(Magics.resolve("I am ${name}, ${add:10,10} years-old.", name="Ann"))
# I am Ann, 20 years-old
```

## Builtin Magic Variables
Magic variables are a set of interpolatable variables that are automatically replaced with their corresponding values when calling `Magics.resolve`. The following magic variables are built-in and can be interpolated during command execution:
```python
# name of the task
task_name = "task_name"

# dirname of the current running backend, same as cattino.where()
run_dir = "~/.cache/cattino/2025-06-03/18-16-24/"

# fullname is composed of the full group name and task name, separated by a slash.
fullname = "group/task_name"
```

## Builtin Magic Constants
Magic constants are a set of predefined string pairs that replace parts of the string. The key difference
of magic constants from magic variables is that magic constants 
```python
# Replace '/' in fullname of tasks with directory separator.
# e.g. if fullname is "group/task_name", then fullpath will be "group\task_name" on Windows and "group/task_name" on Unix-like systems.
fullpath = "${eval:'${fullname}'.replace('/', '%s')}" % os.sep
```


## Builtin Resovlers
Resolvers are functions that can be used within magic strings to perform specific operations or calculations. The following resolvers are built-in and can be used directly in your commands:

```python
# eval: evaluates a Python expression and returns the result
eval = lambda x: eval(x)
```