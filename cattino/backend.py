import asyncio
import builtins
import logging
import re
import click
import dill
import os
import signal
import sys
import fastapi
import uvicorn
from contextlib import asynccontextmanager
from typing import Callable, List, Optional
from collections.abc import Sequence
from fastapi import Depends, FastAPI, File, HTTPException, UploadFile, status
from loguru import logger

from cattino.comms import Request, Response, TaskResponse
from cattino.core.task_scheduler import TaskScheduler
from cattino.tasks.interface import Task, TaskGroup, TaskStatus
from cattino.settings import settings
from cattino.utils import Magics, get_cache_dir, has_param_type, open_redirected_stream


class InterceptHandler(logging.Handler):
    def emit(self, record: logging.LogRecord) -> None:
        try:
            level = logger.level(record.levelname).name
        except ValueError:
            level = record.levelno
        frame, depth = logging.currentframe(), 2
        while frame.f_code.co_filename == logging.__file__:  # type: ignore
            frame = frame.f_back  # type: ignore
            depth += 1

        logger.opt(depth=depth, exception=record.exc_info).log(
            level, record.getMessage()
        )


def setup_logging():
    """
    Replace the fastapi logger with loguru logger.
    """
    logger.configure(extra={"request_id": ""})
    logger.remove()

    log_format = (
        "<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | "
        "<level>{level: ^4}</level> | "
        "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - "
        "<level>{message}</level>"
    )

    logger.add(
        sys.stdout,
        format=log_format,
        level="DEBUG" if settings.debugging else "INFO",
        enqueue=True,
        backtrace=False,
        diagnose=True,
        colorize=True,
    )

    logger_name_list = [name for name in logging.root.manager.loggerDict]
    for logger_name in logger_name_list:
        _logger = logging.getLogger(logger_name)
        _logger.setLevel(logging.INFO)
        _logger.handlers = []
        if "." not in logger_name:
            _logger.addHandler(InterceptHandler())


@asynccontextmanager
async def lifespan(app: FastAPI):
    cache_dir = get_cache_dir("backend")
    os.makedirs(cache_dir, exist_ok=True)
    if app.state.redirect_output:
        sys.stdout = open_redirected_stream(cache_dir, "stdout")
        sys.stderr = open_redirected_stream(cache_dir, "stderr")

    setup_logging()
    shutdown_event = asyncio.Event()
    task_scheduler = TaskScheduler()
    app.state.task_scheduler = task_scheduler

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
                        os.kill(os.getpid(), signal.SIGINT)
                    else:
                        await asyncio.sleep(5)
            except Exception as e:
                logger.exception("Error during task scheduling loop: %s", e)
                await asyncio.sleep(5)

    task_loop = asyncio.create_task(main_loop())

    yield

    shutdown_event.set()
    await task_loop
    await task_scheduler.remove(await task_scheduler.all_tasks)
    if app.state.redirect_output:
        sys.stdout.flush()
        sys.stderr.flush()
        sys.stdout.close()
        sys.stderr.close()


app = FastAPI(lifespan=lifespan)


async def process_tasks(
    name: Optional[str],
    func: Callable,
    allow_status: Optional[Sequence[TaskStatus]] = None,
    use_regex: bool = False,
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

    success, no_op, failure, exception = [], [], [], []
    allow_status = allow_status or builtins.list(TaskStatus)
    if not is_take_sequence_func:
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
                exception.append(f"Some tasks failed: {str(e)}")

    return TaskResponse(
        success=success, no_op=no_op, failure=failure, detail="\n".join(exception)
    )


def load_from_message(message: UploadFile = File(...), request: fastapi.Request = None):  # type: ignore
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


# type: ignore
@app.post("/kill", response_model=TaskResponse)
async def kill(request: Request = Depends(load_from_message)):
    scheduler: TaskScheduler = app.state.task_scheduler
    return await process_tasks(
        request.name,  # type: ignore
        scheduler.terminate,
        [TaskStatus.Running],
        use_regex=request.use_regex,  # type: ignore
        force=request.force,  # type: ignore
    )


@app.post("/remove", response_model=TaskResponse)
async def remove(request: Request = Depends(load_from_message)):
    scheduler: TaskScheduler = app.state.task_scheduler
    return await process_tasks(
        request.name, scheduler.remove, use_regex=request.use_regex  # type: ignore
    )


@app.post("/cancel", response_model=TaskResponse)
async def cancel(request: Request = Depends(load_from_message)):
    scheduler: TaskScheduler = app.state.task_scheduler
    return await process_tasks(
        request.name,  # type: ignore
        scheduler.cancel,
        [TaskStatus.Running, TaskStatus.Waiting],
        use_regex=request.use_regex,  # type: ignore
    )


@app.post("/resume", response_model=TaskResponse)
async def resume(request: Request = Depends(load_from_message)):
    scheduler: TaskScheduler = app.state.task_scheduler
    return await process_tasks(
        request.name,  # type: ignore
        scheduler.resume,
        [TaskStatus.Cancelled],
        use_regex=request.use_regex,  # type: ignore
    )

@app.post("/occupy", response_model=Response)
async def occupy(request: Request = Depends(load_from_message)):
    scheduler: TaskScheduler = app.state.task_scheduler
    device_ids, evil = request.device_ids, request.evil  # type: ignore
    


@app.get("/list", response_model=Response)
async def list(filter: Optional[str] = None, attrs: str = ""):
    attrs = attrs.split()  # type: ignore
    scheduler: TaskScheduler = app.state.task_scheduler
    all_tasks = await scheduler.all_tasks
    filtered_tasks = []
    for task in all_tasks:
        if filter:
            filter_body = Magics.resolve(
                filter,
                task_name=task.name,
                fullname=task.fullname,
                run_dir=getattr(task, "cache_dir", None),
            )
            try:
                filter_fn = eval("lambda task: " + filter_body)
                if filter_fn(task):
                    filtered_tasks.append(task)
            except Exception as e:
                logger.exception(e)
                return Response(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"{filter} is not a valid python expression for task {task.fullname}: {e}",
                )
        else:
            filtered_tasks.append(task)

    return Response(
        status_code=status.HTTP_200_OK,
        results=[
            {
                "name": task.fullname,
                **{attr: getattr(task, attr, None) for attr in attrs},
            }
            for task in filtered_tasks
        ],
    )


@app.post("/set", response_model=Response)
async def set_task_attr(request: Request = Depends(load_from_message)):
    scheduler: TaskScheduler = app.state.task_scheduler
    name, attr, value = request.name, request.attr, request.value  # type: ignore
    task = await scheduler.get_task_object(name)
    if task is None:
        return Response(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Task {name} not found.",
        )
    if not hasattr(task, attr):
        return Response(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Task {name} has no attribute {attr}.",
        )
    if attr not in type(task).SETTABLE_ATTRS:
        return Response(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Task {name} attribute {attr} is not settable. Only {type(task).SETTABLE_ATTRS} are settable.",
        )
    try:
        if isinstance(getattr(task, attr), str):
            setattr(task, attr, value)
        else:
            setattr(task, attr, eval(value))
    except Exception as e:
        logger.exception(e)
        return Response(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Failed to set attribute {attr} for task {name}: {e}",
        )
    return Response(status_code=status.HTTP_200_OK)


@app.post("/create", response_model=TaskResponse)
async def create_task(request: Request = Depends(load_from_message)):
    scheduler: TaskScheduler = app.state.task_scheduler
    success, failure, no_op, exception = [], [], [], []
    for task in request.tasks:  # type: ignore
        fullname = task.fullname
        try:
            await scheduler.dispatch(task)
        except Exception as e:
            logger.exception(e)
            failure.append(fullname)
            exception.append(f"Task failed: {fullname}")
            continue
        if issubclass(type(task), TaskGroup):
            success.extend(
                [t.fullname for t in task.all_tasks if t.status != TaskStatus.Cancelled]
            )
            no_op.extend(
                [t.fullname for t in task.all_tasks if t.status == TaskStatus.Cancelled]
            )
        else:
            if task.status == TaskStatus.Cancelled:
                no_op.append(fullname)
            else:
                success.append(fullname)

    return TaskResponse(
        success=success, no_op=no_op, failure=failure, detail="\n".join(exception)
    )


@app.get("/test", response_model=Response)
async def test_backend():
    return Response(status_code=status.HTTP_200_OK, pid=os.getpid())


@app.get("/test/{name}", response_model=Response)
async def test_task(name: str):
    scheduler: TaskScheduler = app.state.task_scheduler
    if (tasks := await scheduler.get_tasks(name)) is None:
        return Response(status_code=status.HTTP_404_NOT_FOUND, pid=None)
    if len(tasks) != 1:
        return Response(
            status_code=status.HTTP_400_BAD_REQUEST,
            pid=None,
            detail=f"{name} is not a valid name for task.",
        )
    if (task := tasks[0]).status != TaskStatus.Running:
        return Response(status_code=status.HTTP_202_ACCEPTED, pid=None)

    return Response(status_code=status.HTTP_200_OK, pid=getattr(task, "pid", None))


@app.post("/exit", response_model=Response)
async def exit():
    os.kill(os.getpid(), signal.SIGINT)
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
def run(host: Optional[str], port: Optional[int], redirect_output: bool):
    app.state.redirect_output = redirect_output
    uvicorn.run(
        app,
        host=host or settings.host,
        port=port or settings.port,
        access_log=redirect_output and settings.debugging,
        log_config=None,
    )


if __name__ == "__main__":
    run()
