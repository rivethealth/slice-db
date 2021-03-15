import typing

import numpy


class IntSet:
    def __init__(self, dtype):
        self._dtype = dtype
        self._array = numpy.array([], dtype=dtype)

    def add(self, items: typing.List[int]):
        array = numpy.array(items, dtype=self._dtype)
        self._array = numpy.concatenate([self._array, array])
        numpy.sort(self._array)

    def __contains__(self, item: int):
        (i,) = numpy.searchsorted(self._array, [item])
        return i < len(self._array) and self._array[i] == item
