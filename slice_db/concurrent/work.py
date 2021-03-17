import contextlib
import logging
import queue
import threading
import typing

T = typing.TypeVar("T")


class WorkerRunner:
    """
    Runner

    :param int parallelism: Number to of workers to run in parallel
    :param handler: Handler that receives work and optionally yields other work
    :param resource: Resource factory. Can be used to provide thread-specific resources.

    Example:
        def work(x: int):
            print(x)
            while 0 < x:
                x -= 1
                yield x

        runner = WorkerRunner(2, work)
        runner.run([3])

        # 3
        # 2
        # 1
        # 0
        # 0
        # 1
        # 0
        # 0
    """

    def __init__(self, parallelism: int, handler, resource=contextlib.nullcontext):
        self._handler = handler
        self._parallelism = parallelism
        self._resource = resource

    def run(self, items):
        queue_ = queue.LifoQueue()
        status = WorkStatus()

        for item in reversed(items):
            queue_.put(WorkItem(item))

        threads = []
        workers = []
        for _ in range(self._parallelism):
            worker = Worker(queue_, status, self._handler, self._resource)
            workers.append(worker)
            thread = threading.Thread(target=worker.run)
            thread.start()
            threads.append(thread)
        queue_.join()
        for _ in range(self._parallelism):
            queue_.put(END_ITEM)
        for thread in threads:
            thread.join()
        for worker in workers:
            worker.result()


"""
End item
"""
END_ITEM = None


class WorkItem(typing.Generic[T]):
    def __init__(self, value: T):
        self.value = value


class WorkStatus:
    def __init__(self):
        self.running = True


class Worker(typing.Generic[T]):
    def __init__(self, queue: queue.Queue, status: WorkStatus, handler, resource):
        super().__init__()

        self._queue = queue
        self._exception = None
        self._handler = handler
        self._resource = resource
        self._status = status

    def _drain(self):
        """
        Remove items from queue
        """
        while True:
            try:
                if self._queue.get(False) is END_ITEM:
                    break
            except queue.Empty:
                break
            else:
                self._queue.task_done()

    def run(self):
        try:
            with self._resource() as resource:
                self._work(resource)
        except Exception as e:
            self._exception = e
            self._status.running = False
        self._drain()

    def result(self):
        if self._exception is not None:
            raise self._exception

    def _work(self, resource):
        while self._status.running:
            item = self._queue.get()
            try:
                if item is END_ITEM:
                    self._shutdown = True
                    return
                result = self._handler(item.value, resource)
                if result is not None:
                    for item in result:
                        self._queue.put(WorkItem(item))
            finally:
                self._queue.task_done()
