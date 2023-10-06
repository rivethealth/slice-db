import calendar
import datetime
import typing

from ..transform import Transform, TransformContext, Transformer
from .common import create_random


class DateYearTransform(Transform):
    def create(self, context: TransformContext, pepper: bytes, config):
        return _DateYearTransformer(pepper)


class _DateYearTransformer(Transformer):
    def __init__(self, pepper: bytes):
        self._pepper = pepper

    def transform(self, text: typing.Optional[str]):
        if text is None:
            return None

        random = create_random(text.encode("utf-8") + self._pepper)

        date = datetime.date.fromisoformat(text)
        days_in_year = 366 if calendar.isleap(date.year) else 365
        days = random.randrange(days_in_year)
        date = datetime.date(date.year, 1, 1) + datetime.timedelta(days=days)
        return date.isoformat()
