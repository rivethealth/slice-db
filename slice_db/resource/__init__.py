import typing

T = typing.TypeVar("T")

ResourceFactory = typing.Callable[[], typing.ContextManager[T]]
