import asyncio
import contextvars
import functools
import typing


async def to_thread(func, *args, **kwargs):
    """Added in Python 3.9"""
    loop = asyncio.events.get_running_loop()
    ctx = contextvars.copy_context()
    func_call = functools.partial(ctx.run, func, *args, **kwargs)
    return await loop.run_in_executor(None, func_call)


async def wait_success(tasks: typing.Iterable[asyncio.Task]):
    tasks = list(tasks)
    if not tasks:
        return
    try:
        await asyncio.gather(*tasks)
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.wait(tasks)
