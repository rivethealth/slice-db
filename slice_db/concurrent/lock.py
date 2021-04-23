import asyncio


class LifoSemaphore:
    """
    Similar to asyncio.Semaphone, but LIFO
    """

    def __init__(self, value):
        self._loop = asyncio.events.get_event_loop()
        self._value = value
        self._waiters = []

    async def __aenter__(self):
        await self.acquire()

    async def __aexit__(self, exc_type, exc, tb):
        self.release()

    def _wake_up_next(self):
        while self._waiters:
            waiter = self._waiters.pop()
            if not waiter.done():
                waiter.set_result(None)
                return

    async def acquire(self):
        while self._value <= 0:
            fut = self._loop.create_future()
            self._waiters.append(fut)
            try:
                await fut
            except:
                fut.cancel()
                if 0 < self._value and not fut.cancelled():
                    self._wake_up_next()
                raise
        self._value -= 1
        return True

    def release(self):
        self._value += 1
        self._wake_up_next()
