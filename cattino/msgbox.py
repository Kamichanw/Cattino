import asyncio
import click
import dill
import os
import signal
import fastapi
import uvicorn
import logging

from contextlib import asynccontextmanager, redirect_stderr, redirect_stdout, ExitStack
from typing import Optional
from fastapi import Depends, FastAPI, File, HTTPException, UploadFile, status
from loguru import logger

from cattino.comms import Response, Message, MessageListResponse
from cattino.core.task_scheduler import TaskScheduler
from cattino.settings import settings
from cattino.utils import get_cache_dir, get_cattino_home, setup_logger


@asynccontextmanager
async def lifespan(app: FastAPI):
    with ExitStack() as stack:
        if app.state.backend_dir:
            output = stack.enter_context(
                open(
                    os.path.join(app.state.backend_dir, "msgbox.log"), "w", buffering=1
                )
            )
            stack.enter_context(redirect_stdout(output))
            stack.enter_context(redirect_stderr(output))

        setup_logger(colorize=app.state.backend_dir is None)
        yield


app = FastAPI(lifespan=lifespan)


def load_msgbox_request(message: UploadFile = File(...), request: fastapi.Request = None):  # type: ignore
    """
    Load the request from the uploaded file.
    """
    msg = message.file.read()
    try:
        request = dill.loads(msg)
    except Exception as e:
        logger.exception(e)
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Failed to load request: {e}",
        )
    return request


@app.get("/test", response_model=Response)
async def test_backend():
    return Response(
        status_code=status.HTTP_200_OK,
        pid=os.getpid(),
        path=get_cache_dir("backend"),
        home=get_cattino_home(),
    )


@app.post("/exit", response_model=Response)
async def exit():
    os.kill(os.getpid(), signal.SIGINT)
    return Response(status_code=status.HTTP_200_OK)


@app.post("/msg", response_model=Response)
async def receive_message(message: Message = Depends(load_msgbox_request)):
    logger.log(
        logging.getLevelName(message.level),
        (f"{message.tag}: {message.content}" if message.tag else message.content),
    )

    app.state.messages.append(message)

    return Response(status_code=status.HTTP_200_OK)


@app.post("/fetch", response_model=MessageListResponse)
async def fetch():
    messages = app.state.messages
    app.state.messages = []
    return MessageListResponse(status_code=status.HTTP_200_OK, messages=messages)


@click.command()
@click.option(
    "--backend-dir",
    type=str,
    help="Directory to store the log files.",
    required=False,
)
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
def run(host: str | None, port: int | None, backend_dir: str | None):
    app.state.backend_dir = backend_dir
    app.state.messages = []
    logger.debug(
        f"Starting messagebox server on {host or settings.host}:{port or settings.msgbox_port}, redirect_output={backend_dir is not None}"
    )
    uvicorn.run(app, host=host or settings.host, port=port or settings.msgbox_port)


if __name__ == "__main__":
    run()
