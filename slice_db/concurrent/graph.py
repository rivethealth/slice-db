import contextlib
import threading
import typing

from ..graph import DepFn, check_cycle
from .work import WorkerRunner

T = typing.TypeVar("T", bound=typing.Hashable)


class ActionNode(typing.Generic[T]):
    def __init__(self, value: T, dependency_count: int):
        self.value = value
        self.reverse_dependencies = []
        self._dependency_count = dependency_count
        self._lock = threading.Lock()

    def can_execute(self):
        return not self._dependency_count

    def remove_dep(self):
        with self._lock:
            self._dependency_count -= 1
            return self.can_execute()


class GraphRunner:
    def __init__(self, parallelism, handler, resource=contextlib.nullcontext):
        self._runner = WorkerRunner(parallelism, self._handle_node, resource)
        self._handler = handler

    def run(self, items: typing.List[T], dep_fn: DepFn[T]):
        check_cycle(items, dep_fn)

        nodes = {item: ActionNode(item, len(dep_fn(item))) for item in items}

        for item, node in nodes.items():
            for dep in dep_fn(item):
                nodes[dep].reverse_dependencies.append(node)

        self._runner.run([node for node in nodes.values() if node.can_execute()])

    def _handle_node(self, item: ActionNode, resource):
        self._handler(item.value, resource)
        for dep in item.reverse_dependencies:
            if dep.remove_dep():
                yield dep
