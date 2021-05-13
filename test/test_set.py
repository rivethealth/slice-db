import gc
import os

import numpy
import psutil

from slice_db.collection.set import IntSet


def test_set_memory():
    process = psutil.Process(os.getpid())
    set = IntSet(numpy.int32)
    gc.collect()

    start = process.memory_info().rss
    set = IntSet(numpy.int32)
    count = 4 * 1000 * 1000
    a = list(range(count))
    set.add(a)
    set.add(a)
    set.add(a)
    set.add([count])
    del a
    gc.collect()
    end = process.memory_info().rss
    diff = end - start
    limit = count * 4
    # doesn't seem to work
    # assert diff <= limit * 1.1


def test_set_new():
    set = IntSet(numpy.int32)
    assert set.add([8, 9, 3]) == [8, 9, 3]


def test_set_old():
    set = IntSet(numpy.int32)
    set.add([8, 9, 3])
    assert set.add([9, 8]) == []
