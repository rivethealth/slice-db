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

    def _cancel(self):
        for future in self._futures:
            future.cancel()

    def _done(self, future: asyncio.Future):
        try:
            exception = future.exception()
        except asyncio.CancelledError as e:
            exception = e
        if exception is not None and self._exception is None:
            self._exception = exception
            self._cancel()

        self._futures.remove(future)
        if not self._futures:
            self._future.set_result(None)

    def add(self, future: asyncio.Future):
        self._futures.add(future)
        future.add_done_callback(self._done)

    async def finished(self):
        while self._futures:
            self._future = asyncio.Future()
            try:
                await self._future
            except asyncio.CancelledError as e:
                self._exception = e
                self._cancel()
        if self._exception is not None:
            raise self._exception
