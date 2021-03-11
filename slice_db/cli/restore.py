import contextlib

from ..pg import connection, transaction
from ..restore import restore
from .common import open_bytes_read


class NoArgs:
    def __init__(self, fn):
        self._fn = fn
        self._resource = None

    def __enter__(self, *args, **kwargs):
        self._resource = self._fn()
        return self._resource.__enter__(*args, **kwargs)

    def __exit__(self, *args, **kwargs):
        self._resource.__exit__(*args, **kwargs)
        self._resource = None


def restore_main(args):
    if args.parallelism > 1 and args.transaction:
        raise Exception("A single transaction must be disabled for parallelism > 1")

    @contextlib.contextmanager
    def pg_manager():
        with connection("") as conn:
            if args.transaction:
                with transaction(conn) as cur:
                    yield contextlib.nullcontext(cur)
            else:

                @contextlib.contextmanager
                def pg_transaction():
                    with transaction(conn) as cur:
                        yield cur

                yield NoArgs(pg_transaction)

    with open_bytes_read(args.slice) as file:
        restore(args.parallelism, pg_manager, file)
