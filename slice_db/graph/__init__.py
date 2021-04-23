from __future__ import annotations

import typing

T = typing.TypeVar("T")

DepFn = typing.Callable[[T], typing.Iterable[T]]


class CycleError(Exception):
    def __init__(self, nodes):
        super().__init__(CycleError.str(nodes))
        self.nodes = nodes

    @staticmethod
    def str(nodes: typing.List[T]) -> str:
        nodes = nodes + [nodes[0]]
        return " -> ".join(str(node) for node in nodes)


def check_cycle(nodes: typing.List[T], deps_fn: DepFn[T]) -> None:
    """
    Raise CycleError if cycle exists
    """
    visited = set()
    stack = []
    stack_set = set()

    def f(nodes: typing.Iterable[T]):
        for node in nodes:
            if node in visited:
                return

            if node in stack_set:
                i = stack.index(node)
                raise CycleError(stack[i:])

            stack.append(node)
            stack_set.add(node)

            f(deps_fn(node))

            stack.pop()
            stack_set.remove(node)

            visited.add(node)

    f(nodes)
