import asyncio
import contextlib
import typing

from ..graph import DepFn, check_cycle
from . import wait_success

AsyncCallable = typing.Callable[[], typing.Awaitable[None]]

T = typing.TypeVar("T", bound=AsyncCallable)  # And typing.Hashable


class ActionNode:
    def __init__(self, fn: AsyncCallable):
        self.fn = fn
        self.reverse_dependencies = []
        self._dependency_count = 0

    def can_execute(self):
        return not self._dependency_count

    def add_dep(self):
        self._dependency_count += 1

    def remove_dep(self):
        self._dependency_count -= 1


class GraphRunner(typing.Generic[T]):
    def __init__(self, dep_fn: DepFn[T]):
        self._dep_fn = dep_fn

    async def run(self, items: typing.Iterable[T]):
        check_cycle(items, self._dep_fn)

        nodes = {item: ActionNode(item) for item in items}

        for item, node in nodes.items():
            for dep in self._dep_fn(item):
                node.add_dep()
                nodes[dep].reverse_dependencies.append(node)

        await self._process_nodes(nodes.values())

    async def _run_node(self, item: ActionNode):
        await item.fn()
        for dep in item.reverse_dependencies:
            dep.remove_dep()
        await self._process_nodes(item.reverse_dependencies)

    async def _process_nodes(self, items: typing.Iterable[ActionNode]):
        await wait_success(self._run_node(item) for item in items if item.can_execute())
