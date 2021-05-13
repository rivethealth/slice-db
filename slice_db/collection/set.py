import logging
import typing

import numpy


class IntSet:
    def __init__(self, dtype):
        self._dtype = dtype
        self._array = numpy.array([], dtype=dtype)

    def add(self, items: typing.List[int]):
        """
        Add and return the new items
        """
        left = numpy.searchsorted(self._array, items, side="left")
        right = numpy.searchsorted(self._array, items, side="right")
        items = [item for item, i, j in zip(items, left, right) if i == j]
        del left  # otherwise memory test fails
        del right
        if items:
            self._array.resize(len(self._array) + len(items))
            self._array[-len(items) :] = items
            self._array.sort()
        return items
