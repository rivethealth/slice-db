import asyncio
import typing

T = typing.TypeVar("T")

ResourceFactory = typing.Callable[[], typing.ContextManager[T]]

AsyncResourceFactory = typing.Callable[[], typing.AsyncContextManager[T]]
