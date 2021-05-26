import asyncio
import typing


class Queue:
    """
    Wait for a dynamic collection of Futures. If one fails, cancel the rest.
    """

    _tasks: typing.List[asyncio.Task]

    def __init__(self):
        self._exception = None
        self._futures = set()
        self._future = asyncio.Future()

    def _done(self, future: asyncio.Future):
        self._futures.remove(future)
        if self._exception is None:
            self._exception = future.exception()
            if self._exception is not None:
                for task in self.tasks:
                    task.cancel()
        if self._futures:
            return

        if self._exception is None:
            self._future.set_result(None)
        else:
            self._future.set_exception(self._exception)

    def add(self, future: asyncio.Future):
        self._futures.add(future)
        future.add_done_callback(self._done)

    async def finished(self):
        await self._future
