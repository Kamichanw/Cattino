import asyncio
import logging
import click
import dill
import os
import signal
import sys
import fastapi
import uvicorn
from contextlib import asynccontextmanager
from typing import Callable, Optional
from collections.abc import Sequence
from fastapi import Depends, FastAPI, File, HTTPException, UploadFile, status
from loguru import logger

from catin.comms import Request, Response, TaskResponse
from catin.core.task_scheduler import TaskScheduler
from catin.tasks.interface import Task, TaskGroup, TaskStatus
from catin.settings import settings
from catin.utils import get_cache_dir, has_param_type, open_redirected_stream


class InterceptHandler(logging.Handler):
    def emit(self, record: logging.LogRecord) -> None:
        try:
            level = logger.level(record.levelname).name
        except ValueError:
            level = record.levelno
        frame, depth = logging.currentframe(), 2
        while frame.f_code.co_filename == logging.__file__:
            frame = frame.f_back
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
                await task_scheduler.step()
                await asyncio.sleep(5)
            except Exception as e:
                logger.exception("Error during task scheduling loop: %s", e)

    task_loop = asyncio.create_task(main_loop())

    yield

    shutdown_event.set()
    await task_scheduler.remove(await task_scheduler.all_tasks)
    await task_loop

    if app.state.redirect_output:
        sys.stdout.flush()
        sys.stderr.flush()
        sys.stdout.close()
        sys.stderr.close()


app = FastAPI(lifespan=lifespan)


async def process_tasks(
    task_names: Sequence[str],
    func: Callable,
    allow_status: Optional[Sequence[TaskStatus]] = None,
    **kwargs,
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

    selected_tasks = []
    success, no_op, failure, not_found, exception = [], [], [], [], []
    allow_status = allow_status or list(TaskStatus)

    for name in task_names:
        task = await scheduler.get_task(name)
        if task is None:
            not_found.append(name)
        elif task.status in allow_status:
            if is_take_sequence_func:
                selected_tasks.append(task)
            else:
                try:
                    await func(task, **kwargs)
                    success.append(name)
                except Exception as e:
                    logger.exception(e)
                    exception.append(f"Task {task.fullname} failed: {str(e)}")
                    failure.append(name)
        else:
            no_op.append(name)

    if is_take_sequence_func:
        # run in batch, if any task failed, we will mark all tasks as failure
        selected_task_names = [task.name for task in selected_tasks]
        try:
            await func(selected_tasks, **kwargs)
            success.extend(selected_task_names)
        except Exception as e:
            logger.exception(e)
            exception.append(f"Some tasks failed: {str(e)}")
            failure.extend(selected_task_names)

    detail = "\n".join(exception)
    if not_found and not success and not no_op and not failure:
        # if all tasks are not found, we will return 404
        return TaskResponse(
            failure=not_found,
            status_code=status.HTTP_404_NOT_FOUND,
            detail=detail,
        )

    failure.extend(not_found)
    return TaskResponse(success=success, no_op=no_op, failure=failure, detail=detail)


def load_from_message(message: UploadFile = File(...), request: fastapi.Request = None):
    """
    Load the request from the uploaded file.
    """
    msg = message.file.read()
    old_path = sys.path
    # add extra paths to load user-defined modules
    extra_modules = request.headers.get("X-Extra-Path", None)
    if extra_modules:
        sys.path = list(set(extra_modules.split(",") + sys.path))
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
async def kill(request: Request = Depends(load_from_message)):
    scheduler: TaskScheduler = app.state.task_scheduler
    task_names = request.task_names or [
        task.name for task in await scheduler.running_tasks
    ]
    return await process_tasks(
        task_names, scheduler.terminate, [TaskStatus.Running], force=request.force
    )


@app.post("/remove", response_model=TaskResponse)
async def remove(request: Request = Depends(load_from_message)):
    scheduler: TaskScheduler = app.state.task_scheduler
    task_names = request.task_names or [task.name for task in await scheduler.all_tasks]
    return await process_tasks(task_names, scheduler.remove)


@app.post("/suspend", response_model=TaskResponse)
async def suspend(request: Request = Depends(load_from_message)):
    scheduler: TaskScheduler = app.state.task_scheduler
    task_names = request.task_names or [task.name for task in await scheduler.all_tasks]
    return await process_tasks(
        task_names,
        scheduler.suspend,
        [TaskStatus.Running, TaskStatus.Waiting],
    )


@app.post("/resume", response_model=TaskResponse)
async def resume(request: Request = Depends(load_from_message)):
    scheduler: TaskScheduler = app.state.task_scheduler
    task_names = request.task_names or [task.name for task in await scheduler.all_tasks]
    return await process_tasks(
        task_names,
        scheduler.resume,
        [TaskStatus.Suspended],
    )


@app.post("/create", response_model=TaskResponse)
async def create_task(request: Request = Depends(load_from_message)):
    scheduler: TaskScheduler = app.state.task_scheduler
    success, failure, exception = [], [], []
    for task in request.tasks:
        name = task.name
        try:
            await scheduler.dispatch(task)
            success.append(name)
        except Exception as e:
            logger.exception(e)
            failure.append(name)
            exception.append(f"Task failed: {task.fullname}")

    return TaskResponse(success=success, failure=failure, detail="\n".join(exception))


@app.post("/test", response_model=Response)
async def test(request: Request = Depends(load_from_message)):
    scheduler: TaskScheduler = app.state.task_scheduler

    if request.name is None or request.name == "backend":
        return Response(status_code=status.HTTP_200_OK, pid=os.getpid())

    task = await scheduler.get_task(request.name)
    if task is None:
        return Response(status_code=status.HTTP_404_NOT_FOUND, pid=None)

    if task.status != TaskStatus.Running:
        return Response(status_code=status.HTTP_202_ACCEPTED, pid=None)

    return Response(status_code=status.HTTP_200_OK, pid=getattr(task, "pid", None))


@app.post("/exit", response_model=Response)
async def exit():
    scheduler: TaskScheduler = app.state.task_scheduler
    await scheduler.remove(await scheduler.all_tasks)
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
