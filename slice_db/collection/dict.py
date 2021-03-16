import collections
import typing

K = typing.TypeVar("K")
T = typing.TypeVar("T")


def groups(
    items: typing.Iterable[T], key_fn: typing.Callable[[T], K]
) -> typing.Dict[K, T]:
    d = collections.defaultdict(list)
    for item in items:
        d[key_fn(item)].append(item)
    return d
