import asyncio
import builtins
import re
import click
import dill
import os
import signal
import sys
import fastapi
import uvicorn

from contextlib import asynccontextmanager, redirect_stderr, redirect_stdout, ExitStack
from typing import Callable
from collections.abc import Sequence
from fastapi import Depends, FastAPI, File, HTTPException, UploadFile, status
from loguru import logger

from cattino.comms import (
    BackendRequest,
    Response,
    TaskResponse,
    Transmittable,
    start_msgbox,
    send_msg_on_error,
)
from cattino.core.task_scheduler import TaskScheduler
from cattino.tasks.interface import Task, TaskGroup, TaskStatus
from cattino.settings import settings
from cattino.utils import (
    Magics,
    get_cache_dir,
    get_cattino_home,
    has_param_type,
    open_redirected_stream,
    setup_logger,
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    cache_dir = get_cache_dir("backend")
    os.makedirs(cache_dir, exist_ok=True)
    with ExitStack() as stack:
        if app.state.redirect_output:
            stdout = stack.enter_context(open_redirected_stream(cache_dir, "stdout"))
            stderr = stack.enter_context(open_redirected_stream(cache_dir, "stderr"))
            stack.enter_context(redirect_stdout(stdout))
            stack.enter_context(redirect_stderr(stderr))

        setup_logger(
            not app.state.redirect_output
        )  # do not colorize if redirecting output to files
        shutdown_event = asyncio.Event()
        task_scheduler = TaskScheduler()
        app.state.task_scheduler = task_scheduler
        app.state.shutdown_event = shutdown_event
        msgbox_proc = await start_msgbox(
            app.state.host,
            settings.msgbox_port,
            cache_dir if app.state.redirect_output else None,
        )

        # create main loop to schedule tasks
        async def main_loop():
            while not shutdown_event.is_set():
                try:
                    if not await task_scheduler.step():
                        if (
                            settings.shutdown_on_complete
                            and app.state.redirect_output  # if backend is running in background
                            and not await task_scheduler.is_running
                        ):
                            logger.info("All tasks are done, shutting down...")
                            shutdown_event.set()
                        else:
                            await asyncio.sleep(5)
                except Exception as e:
                    logger.exception(e)
            os.kill(os.getpid(), signal.SIGINT)

        tasks = [asyncio.create_task(main_loop())]

        # read from msgbox stderr and stdout if not redirecting output
        if not app.state.redirect_output:

            async def read_stream(stream):
                while not shutdown_event.is_set():
                    if line := await stream.readline():
                        print(line.decode("utf-8").rstrip())
                    else:
                        await asyncio.sleep(0.5)

            if msgbox_proc.returncode is not None:
                logger.info(
                    "Message box is not running, some notifications may not be able to be seen in time."
                )
            else:
                if msgbox_proc.stdout:
                    tasks.append(asyncio.create_task(read_stream(msgbox_proc.stdout)))
                if msgbox_proc.stderr:
                    tasks.append(asyncio.create_task(read_stream(msgbox_proc.stderr)))

        yield

        shutdown_event.set()
        if msgbox_proc.returncode is None:
            msgbox_proc.terminate()
        await asyncio.gather(*tasks)
        await task_scheduler.remove(await task_scheduler.all_tasks)


app = FastAPI(lifespan=lifespan)


def _filter_tasks(tasks: list, filter_expr: str) -> list:
    filtered = []
    for task in tasks:
        filter_body = Magics.resolve(
            filter_expr,
            task_name=task.name,
            fullname=task.fullname,
            run_dir=get_cache_dir(),
        )
        try:
            filter_fn = eval("lambda task: " + filter_body)
            if filter_fn(task):
                filtered.append(task)
        except Exception as e:
            logger.exception(e)
            raise ValueError(
                f"{filter_expr} is not a valid python expression for task {task.fullname}: {e}"
            )
    return filtered


async def process_tasks(
    name: str | None,
    func: Callable,
    allow_status: Sequence[TaskStatus] | None = None,
    use_regex: bool = False,
    filter: str | None = None,
    **func_kwargs,
):
    scheduler: TaskScheduler = app.state.task_scheduler

    # determine whether the first arg type of the func is a sequence
    # if the func take a sequence as the first argument, we will pass the selected tasks as input
    # otherwise, we will pass the task one by one and try to capture the exception
    assert asyncio.iscoroutinefunction(func), "func must be a coroutine function"
    is_take_sequence_func = has_param_type(func, (Sequence,), 0)
    is_take_task_func = has_param_type(func, (Task, TaskGroup), 0)
    assert (
        is_take_task_func or is_take_sequence_func
    ), "func must take a task or a sequence of tasks as the first argument"
    tasks = (
        await scheduler.get_tasks(re.compile(name) if use_regex else name)
        if name
        else await scheduler.all_tasks
    )
    if tasks is None:
        return Response(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Task {name} not found",
        )

    # apply filter expression if provided
    if filter:
        try:
            tasks = _filter_tasks(tasks, filter)
        except ValueError as e:
            return Response(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))

    success, failure, no_op = [], [], []
    exception = None
    allow_status = allow_status or builtins.list(TaskStatus)
    if not is_take_sequence_func:
        exception = []
        for task in tasks:
            if task.status in allow_status:
                try:
                    await func(task, **func_kwargs)
                    success.append(task.fullname)
                except Exception as e:
                    logger.exception(e)
                    exception.append(f"Task {task.fullname} failed: {str(e)}")
                    failure.append(task.fullname)
            else:
                no_op.append(task.fullname)
    else:
        no_op.extend([t.fullname for t in tasks if t.status not in allow_status])
        if tasks := [t for t in tasks if t.status in allow_status]:
            try:
                await func(tasks, **func_kwargs)
                success.extend([t.fullname for t in tasks])
            except Exception as e:
                logger.exception(e)
                failure.extend([t.fullname for t in tasks])
                exception = str(e)

    return TaskResponse(
        success=success, failure=failure, no_op=no_op, exception=exception
    )


def load_backend_request(message: UploadFile = File(...), request: fastapi.Request = None):  # type: ignore
    """
    Load the request from the uploaded file.
    """
    msg = message.file.read()
    old_path = sys.path
    # add extra paths to load user-defined modules
    if extra_modules := request.headers.get("X-Extra-Path", None):
        sys.path = extra_modules.split(",") + sys.path
    try:
        request = dill.loads(msg)
    except Exception as e:
        logger.exception(e)
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Failed to load request: {e}",
        )
    sys.path = old_path
    return request


@app.post("/kill", response_model=TaskResponse)
@send_msg_on_error(tag="backend")
async def kill(data: Transmittable = Depends(load_backend_request)):
    scheduler: TaskScheduler = app.state.task_scheduler
    return await process_tasks(
        data.name,  # type: ignore
        scheduler.terminate,
        [TaskStatus.Running],
        use_regex=data.use_regex,  # type: ignore
        filter=data.filter,  # type: ignore
        force=data.force,  # type: ignore
    )


@app.post("/remove", response_model=TaskResponse)
@send_msg_on_error(tag="backend")
async def remove(data: Transmittable = Depends(load_backend_request)):
    scheduler: TaskScheduler = app.state.task_scheduler
    return await process_tasks(
        data.name, scheduler.remove, use_regex=data.use_regex, filter=data.filter  # type: ignore
    )


@app.post("/cancel", response_model=TaskResponse)
@send_msg_on_error(tag="backend")
async def cancel(request: BackendRequest = Depends(load_backend_request)):
    scheduler: TaskScheduler = app.state.task_scheduler
    return await process_tasks(
        request.name,  # type: ignore
        scheduler.cancel,
        [TaskStatus.Running, TaskStatus.Waiting],
        use_regex=request.use_regex,  # type: ignore
        filter=request.filter,  # type: ignore
    )


@app.post("/resume", response_model=TaskResponse)
@send_msg_on_error(tag="backend")
async def resume(request: BackendRequest = Depends(load_backend_request)):
    scheduler: TaskScheduler = app.state.task_scheduler
    return await process_tasks(
        request.name,  # type: ignore
        scheduler.resume,
        [TaskStatus.Cancelled, TaskStatus.Failed, TaskStatus.Done],
        use_regex=request.use_regex,  # type: ignore
        filter=request.filter,  # type: ignore
    )


@app.get("/list", response_model=Response)
@send_msg_on_error(tag="backend")
async def list(
    name: str | None = None,
    use_regex: bool = False,
    filter: str | None = None,
    attrs: str = "",
):
    attrs = attrs.split()  # type: ignore
    scheduler: TaskScheduler = app.state.task_scheduler

    # select tasks by name (supports regex when use_regex is True)
    if name:
        tasks = await scheduler.get_tasks(re.compile(name) if use_regex else name)
        if tasks is None:
            filtered_tasks = []
        else:
            filtered_tasks = tasks
    else:
        filtered_tasks = await scheduler.all_tasks

    # apply filter expression if provided
    if filter:
        try:
            filtered_tasks = _filter_tasks(filtered_tasks, filter)
        except ValueError as e:
            return Response(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))

    def _get_nested_attr(obj, attr_path: str):
        """Resolve dotted attribute path (e.g. 'a.b.c') on obj, returning None if any part missing."""
        if not attr_path:
            return None
        val = obj
        for part in attr_path.split("."):
            try:
                val = getattr(val, part)
            except Exception:
                return None
        return val

    return Response(
        status_code=status.HTTP_200_OK,
        results=[
            {attr: _get_nested_attr(task, attr) for attr in attrs}
            for task in filtered_tasks
        ],
    )


@app.post("/set", response_model=Response)
@send_msg_on_error(tag="backend")
async def set_task_attr(request: BackendRequest = Depends(load_backend_request)):
    """Set attributes on one or more tasks. Supports nested attribute paths like `a.b` and
    selection via regex or filter expressions.
    """
    name, attr, value = request.name, request.attr, request.value  # type: ignore
    use_regex = getattr(request, "use_regex", False)
    filter_expr = getattr(request, "filter", None)

    if not attr:
        return Response(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Attribute name is required.",
        )

    async def _set_attr(tasks: builtins.list[Task | TaskGroup]):
        # tasks is a sequence of Task
        for task in tasks:
            parts = attr.split(".")
            top = parts[0]
            # ensure the top-level attribute is settable on this task type
            if top not in type(task).SETTABLE_ATTRS:
                raise ValueError(
                    f"Task {task.fullname} attribute {top} is not settable. Only {type(task).SETTABLE_ATTRS} are settable."
                )

            cur = task
            # traverse intermediate attributes
            for p in parts[:-1]:
                if not hasattr(cur, p):
                    raise AttributeError(
                        f"Task {task.fullname} has no attribute {p} while resolving {attr}."
                    )
                cur = getattr(cur, p)

            last = parts[-1]
            if not hasattr(cur, last):
                raise AttributeError(
                    f"Task {task.fullname} has no attribute {last} for {attr}."
                )

            old = getattr(cur, last)
            if isinstance(old, str):
                setattr(cur, last, value)
            else:
                setattr(cur, last, eval(value))

    # delegate selection and per-task error handling to process_tasks
    return await process_tasks(name, _set_attr, use_regex=use_regex, filter=filter_expr)


@app.post("/create", response_model=TaskResponse)
@send_msg_on_error(tag="backend")
async def create_task(request: BackendRequest = Depends(load_backend_request)):
    scheduler: TaskScheduler = app.state.task_scheduler
    success, failure, exception, no_op = [], [], [], []
    for task in request.tasks:  # type: ignore
        fullname = task.fullname
        try:
            await scheduler.dispatch(task)
        except Exception as e:
            logger.exception(e)
            failure.append(fullname)
            exception.append(str(e))
            continue
        if isinstance(task, TaskGroup):
            no_op.extend(
                [t.fullname for t in task.all_tasks if t.status == TaskStatus.Cancelled]
            )
            success.extend(
                [t.fullname for t in task.all_tasks if t.status != TaskStatus.Cancelled]
            )
        else:
            if task.status == TaskStatus.Cancelled:
                no_op.append(fullname)
            else:
                success.append(fullname)

    return TaskResponse(
        success=success, failure=failure, exception=exception, no_op=no_op
    )


@app.get("/test", response_model=Response)
@send_msg_on_error(tag="backend")
async def test_backend():
    return Response(
        status_code=status.HTTP_200_OK,
        pid=os.getpid(),
        path=get_cache_dir("backend"),
        home=get_cattino_home(),
    )


@app.post("/exit", response_model=Response)
async def exit():
    scheduler: TaskScheduler = app.state.task_scheduler
    await scheduler.remove(await scheduler.all_tasks)
    app.state.shutdown_event.set()
    return Response(status_code=status.HTTP_200_OK)


@click.command()
@click.option(
    "--host",
    type=str,
    required=False,
    help="Host to run the backend server on.",
)
@click.option(
    "--port",
    type=int,
    required=False,
    help="Port to run the backend server on.",
)
@click.option(
    "--redirect-output",
    type=bool,
    is_flag=True,
    default=False,
    help="Whether to redirect backend outputs to files.",
)
def run(host: str | None, port: int | None, redirect_output: bool):
    app.state.redirect_output = redirect_output
    app.state.host = host or settings.host
    logger.debug(
        f"Starting backend server on {host or settings.host}:{port or settings.port}, redirect_output={redirect_output}"
    )
    uvicorn.run(
        app,
        host=app.state.host,
        port=port or settings.port,
        access_log=settings.debugging,
        log_config=None,
    )


if __name__ == "__main__":
    run()
