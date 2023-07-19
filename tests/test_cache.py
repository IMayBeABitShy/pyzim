"""
Tests for L{pyzim.cache}.
"""
import unittest

from pyzim.cache import _DoubleLinkedList, _DoubleLinkedListElement
from pyzim.cache import BaseCache, LastAccessCache, TopAccessCache, HybridCache, NoOpCache

from .base import TestBase


def query_cache_n_times(cache, key, value, n):
    """
    Query the cache n times.

    @param cache: cache to query
    @type cache: L{pyzim.cache.BaseCache}
    @param key: key to query
    @type key: hashable
    @param value: expected value, will be pushed if not in cache
    @type value: any
    @param n: number of times to query cache
    @type n: L{int}
    @return: whether the element has been added to the cache or not
    @rtype: L{bool}
    """
    assert isinstance(cache, BaseCache)
    assert isinstance(n, int) and n >= 0
    did_add = False
    for i in range(n):
        if cache.has(key):
            assert cache.get(key) == value
        else:
            assert not did_add
            did_push = cache.push(key, value)
            if did_push:
                did_add = True
            else:
                # should not become False once it was True
                assert not did_add
    return did_add


class DoubleLinkedListTests(unittest.TestCase, TestBase):
    """
    Tests for L{pyzim.cache._DoubleLinkedList}.
    """
    def test_append(self):
        """
        Test L{pyzim.cache._DoubleLinkedList.append} and some related methods.
        """
        dll = _DoubleLinkedList()
        # first, add 4 elements one after another
        self.assertEqual(dll.length, 0)
        self.assertEqual(dll.to_list(), [])
        self.assertIsNone(dll.head)
        self.assertIsNone(dll.tail)
        dll.append("a")
        self.assertEqual(dll.length, 1)
        self.assertEqual(dll.to_list(), ["a"])
        self.assertIsNotNone(dll.head)
        self.assertIsNotNone(dll.tail)
        self.assertIs(dll.head, dll.tail)
        element_b = dll.append("b")
        self.assertIsInstance(element_b, _DoubleLinkedListElement)
        self.assertEqual(dll.length, 2)
        self.assertEqual(dll.to_list(), ["a", "b"])
        self.assertIsNotNone(dll.head)
        self.assertIsNotNone(dll.tail)
        self.assertIsNot(dll.head, dll.tail)
        dll.append("c")
        self.assertEqual(dll.length, 3)
        self.assertEqual(dll.to_list(), ["a", "b", "c"])
        dll.append("d")
        self.assertEqual(dll.length, 4)
        self.assertEqual(dll.to_list(), ["a", "b", "c", "d"])
        # remove last element
        dll.remove_last()
        self.assertEqual(dll.length, 3)
        self.assertEqual(dll.to_list(), ["a", "b", "c"])
        # ensure append still works
        dll.append("e")
        self.assertEqual(dll.length, 4)
        self.assertEqual(dll.to_list(), ["a", "b", "c", "e"])
        # test that append works with an existing element
        dll.remove_element(element_b)
        self.assertEqual(dll.length, 3)
        self.assertEqual(dll.to_list(), ["a", "c", "e"])
        dll.append(element_b)
        self.assertEqual(dll.length, 4)
        self.assertEqual(dll.to_list(), ["a", "c", "e", "b"])
        # test append with a new element
        element_f = _DoubleLinkedListElement(dll, "f")
        dll.append(element_f)
        self.assertEqual(dll.length, 5)
        self.assertEqual(dll.to_list(), ["a", "c", "e", "b", "f"])

    def test_prepend(self):
        """
        Test L{pyzim.cache._DoubleLinkedList.prepend} and some related methods.
        """
        dll = _DoubleLinkedList()
        # first, add 4 elements one after another
        self.assertEqual(dll.length, 0)
        self.assertEqual(dll.to_list(), [])
        self.assertIsNone(dll.head)
        self.assertIsNone(dll.tail)
        dll.prepend("a")
        self.assertEqual(dll.length, 1)
        self.assertEqual(dll.to_list(), ["a"])
        self.assertIsNotNone(dll.head)
        self.assertIsNotNone(dll.tail)
        self.assertIs(dll.head, dll.tail)
        element_b = dll.prepend("b")
        self.assertIsInstance(element_b, _DoubleLinkedListElement)
        self.assertEqual(dll.length, 2)
        self.assertEqual(dll.to_list(), ["b", "a"])
        self.assertIsNotNone(dll.head)
        self.assertIsNotNone(dll.tail)
        self.assertIsNot(dll.head, dll.tail)
        dll.prepend("c")
        self.assertEqual(dll.length, 3)
        self.assertEqual(dll.to_list(), ["c", "b", "a"])
        dll.prepend("d")
        self.assertEqual(dll.length, 4)
        self.assertEqual(dll.to_list(), ["d", "c", "b", "a"])
        # remove first element
        dll.remove_first()
        self.assertEqual(dll.length, 3)
        self.assertEqual(dll.to_list(), ["c", "b", "a"])
        # ensure prepend still works
        dll.prepend("e")
        self.assertEqual(dll.length, 4)
        self.assertEqual(dll.to_list(), ["e", "c", "b", "a"])
        # test that prepend works with an existing element
        dll.remove_element(element_b)
        self.assertEqual(dll.length, 3)
        self.assertEqual(dll.to_list(), ["e", "c", "a"])
        dll.prepend(element_b)
        self.assertEqual(dll.length, 4)
        self.assertEqual(dll.to_list(), ["b", "e", "c", "a"])
        # test prepend with a new element
        element_f = _DoubleLinkedListElement(dll, "f")
        dll.prepend(element_f)
        self.assertEqual(dll.length, 5)
        self.assertEqual(dll.to_list(), ["f", "b", "e", "c", "a"])

    def test_clear(self):
        """
        Test L{pyzim.cache._DoubleLinkedList.clear}.
        """
        dll = _DoubleLinkedList()
        for v in ("a", "b", "c", "d"):
            dll.append(v)
        self.assertEqual(dll.length, 4)
        self.assertEqual(dll.to_list(), ["a", "b", "c", "d"])
        dll.clear()
        self.assertEqual(dll.length, 0)
        self.assertEqual(dll.to_list(), [])
        self.assertIsNone(dll.head)
        self.assertIsNone(dll.tail)
        # ensure that append still works
        dll.append("e")
        self.assertEqual(dll.length, 1)
        self.assertEqual(dll.to_list(), ["e"])
        # ensure that prepend still works
        dll.clear()  # clear again
        dll.prepend("f")
        self.assertEqual(dll.length, 1)
        self.assertEqual(dll.to_list(), ["f"])

    def test_remove_last(self):
        """
        Test L{pyzim.cache._DoubleLinkedList.remove_last}.
        """
        dll = _DoubleLinkedList()
        for v in ("a", "b", "c", "d"):
            dll.append(v)
        self.assertEqual(dll.length, 4)
        self.assertEqual(dll.to_list(), ["a", "b", "c", "d"])
        # remove last element
        dll.remove_last()
        self.assertEqual(dll.length, 3)
        self.assertEqual(dll.to_list(), ["a", "b", "c"])
        # ensure append still works
        dll.append("e")
        self.assertEqual(dll.length, 4)
        self.assertEqual(dll.to_list(), ["a", "b", "c", "e"])
        # remove last element twice
        dll.remove_last()
        dll.remove_last()
        self.assertEqual(dll.length, 2)
        self.assertEqual(dll.to_list(), ["a", "b"])
        # repeat
        dll.remove_last()
        dll.remove_last()
        self.assertEqual(dll.length, 0)
        self.assertEqual(dll.to_list(), [])
        # ensure error on empty remove
        with self.assertRaises(IndexError):
            dll.remove_last()
        # ensure append still works
        dll.append("f")
        self.assertEqual(dll.length, 1)
        self.assertEqual(dll.to_list(), ["f"])

    def test_remove_first(self):
        """
        Test L{pyzim.cache._DoubleLinkedList.remove_first}.
        """
        dll = _DoubleLinkedList()
        for v in ("a", "b", "c", "d"):
            dll.append(v)
        self.assertEqual(dll.length, 4)
        self.assertEqual(dll.to_list(), ["a", "b", "c", "d"])
        # remove first element
        dll.remove_first()
        self.assertEqual(dll.length, 3)
        self.assertEqual(dll.to_list(), ["b", "c", "d"])
        # ensure prepend still works
        dll.prepend("e")
        self.assertEqual(dll.length, 4)
        self.assertEqual(dll.to_list(), ["e", "b", "c", "d"])
        # remove first element twice
        dll.remove_first()
        dll.remove_first()
        self.assertEqual(dll.length, 2)
        self.assertEqual(dll.to_list(), ["c", "d"])
        # repeat
        dll.remove_first()
        dll.remove_first()
        self.assertEqual(dll.length, 0)
        self.assertEqual(dll.to_list(), [])
        self.assertIsNone(dll.head)
        self.assertIsNone(dll.tail)
        # ensure error on empty remove
        with self.assertRaises(IndexError):
            dll.remove_first()
        # ensure prepend still works
        dll.prepend("f")
        self.assertEqual(dll.length, 1)
        self.assertEqual(dll.to_list(), ["f"])

    def test_remove_element(self):
        """
        Test L{pyzim.cache._DoubleLinkedList.remove_element}.
        """
        dll = _DoubleLinkedList()
        element_a = dll.append("a")
        element_b = dll.append("b")
        element_c = dll.append("c")
        element_d = dll.append("d")
        element_e = dll.append("e")
        self.assertEqual(dll.length, 5)
        self.assertEqual(dll.to_list(), ["a", "b", "c", "d", "e"])
        # remove element in middle
        dll.remove_element(element_c)
        self.assertEqual(dll.length, 4)
        self.assertEqual(dll.to_list(), ["a", "b", "d", "e"])
        # remove element at start
        dll.remove_element(element_a)
        self.assertEqual(dll.length, 3)
        self.assertEqual(dll.to_list(), ["b", "d", "e"])
        # ensure prepend still works
        element_f = dll.prepend("f")
        self.assertEqual(dll.length, 4)
        self.assertEqual(dll.to_list(), ["f", "b", "d", "e"])
        # remove element at end
        dll.remove_element(element_e)
        self.assertEqual(dll.length, 3)
        self.assertEqual(dll.to_list(), ["f", "b", "d"])
        # ensure append still works
        element_g = dll.append("g")
        self.assertEqual(dll.length, 4)
        self.assertEqual(dll.to_list(), ["f", "b", "d", "g"])
        # remove outer two element, leaving only two elements
        dll.remove_element(element_f)
        dll.remove_element(element_g)
        self.assertEqual(dll.length, 2)
        self.assertEqual(dll.to_list(), ["b", "d"])
        # ensure remove still works when only two elements in list (head and tail)
        dll.remove_element(element_b)
        self.assertEqual(dll.length, 1)
        self.assertEqual(dll.to_list(), ["d"])
        dll.remove_element(element_d)
        self.assertEqual(dll.length, 0)
        self.assertEqual(dll.to_list(), [])
        self.assertIsNone(dll.head)
        self.assertIsNone(dll.tail)
        # ensure append still works
        dll.prepend("h")
        self.assertEqual(dll.length, 1)
        self.assertEqual(dll.to_list(), ["h"])
        # test error on element that is not part of a list
        with self.assertRaises(ValueError):
            dll.remove_element(element_a)
        with self.assertRaises(ValueError):
            dll2 = _DoubleLinkedList()
            dll2.remove_element(element_a)

    def test_iter(self):
        """
        Test L{pyzim.cache._DoubleLinkedList.__iter__}.
        """
        dll = _DoubleLinkedList()
        for v in ("a", "b", "c", "d"):
            dll.append(v)
        self.assertEqual(dll.length, 4)
        self.assertEqual(dll.to_list(), ["a", "b", "c", "d"])
        dll = _DoubleLinkedList()
        for v in ("a", "b", "c", "d"):
            dll.append(v)
        self.assertEqual(dll.length, 4)
        self.assertEqual(list(dll), ["a", "b", "c", "d"])
        dll = _DoubleLinkedList()
        for v in ("a", "b", "c", "d"):
            dll.append(v)
        self.assertEqual(dll.length, 4)
        self.assertEqual(dll.to_list(), list(dll))
        dll = _DoubleLinkedList()
        for v in ("a", "b", "c", "d"):
            dll.append(v)
        self.assertEqual(dll.length, 4)
        self.assertEqual([e for e in dll], ["a", "b", "c", "d"])

    def test_repr(self):
        """
        Test L{pyzim.cache.DoubleLinkedList.__repr__}.
        """
        rawlist = ["1", "2", 3, 4, "five"]
        dll = _DoubleLinkedList(rawlist)
        s = repr(dll)
        self.assertIn(dll.__class__.__name__, s)
        self.assertIn(repr(rawlist), s)

        # also check for the element
        element = dll.head
        s = repr(element)
        self.assertIn(element.__class__.__name__, s)
        self.assertIn(repr(element.value), s)

    def test_swap_forward(self):
        """
        Test L{pyzim.cache._DoubleLinkedListElement.swap_forward}.
        """
        dll = _DoubleLinkedList()
        element_a = dll.append("a")
        element_b = dll.append("b")
        element_c = dll.append("c")
        element_d = dll.append("d")
        self.assertEqual(dll.length, 4)
        self.assertEqual(dll.to_list(), ["a", "b", "c", "d"])
        # swapping a should not have any effect
        element_a.swap_forward()
        self.assertEqual(dll.length, 4)
        self.assertEqual(dll.to_list(), ["a", "b", "c", "d"])
        self.assertIs(dll.head, element_a)
        self.assertIs(dll.tail, element_d)
        # swap a and b
        element_b.swap_forward()
        self.assertEqual(dll.length, 4)
        self.assertEqual(dll.to_list(), ["b", "a", "c", "d"])
        self.assertIs(dll.head, element_b)
        self.assertIs(dll.tail, element_d)
        # swap c forward
        element_c.swap_forward()
        self.assertEqual(dll.length, 4)
        self.assertEqual(dll.to_list(), ["b", "c", "a", "d"])
        self.assertIs(dll.head, element_b)
        self.assertIs(dll.tail, element_d)
        # swap d forward twice
        element_d.swap_forward()
        element_d.swap_forward()
        self.assertEqual(dll.length, 4)
        self.assertEqual(dll.to_list(), ["b", "d", "c", "a"])
        self.assertIs(dll.head, element_b)
        self.assertIs(dll.tail, element_a)


class BaseCacheTests(unittest.TestCase, TestBase):
    """
    Tests for L{pyzim.cache.BaseCache}.

    This is not a general test suite for all subclasses of L{pyzim.cache.BaseCache}!
    """
    def test_raise_not_implemented(self):
        """
        Ensure the various methods raise L{NotImplementedError}.
        """
        cache = BaseCache()
        with self.assertRaises(NotImplementedError):
            cache.has(1)
        with self.assertRaises(NotImplementedError):
            cache.get(1)
        with self.assertRaises(NotImplementedError):
            cache.push(1, "a")
        with self.assertRaises(NotImplementedError):
            cache.clear()


class NoOpCacheTests(unittest.TestCase, TestBase):
    """
    Tests for L{pyzim.cache.NoOpCacheTests}.
    """
    def test_no_cache(self):
        """
        Test caching behavior.
        """
        cache = NoOpCache()

        self.assertFalse(cache.has(1))
        self.assertFalse(cache.has(2))
        self.assertFalse(cache.push(1, "a"))
        self.assertFalse(cache.has(1))
        with self.assertRaises(KeyError):
            cache.get(1)
        cache.clear()
        self.assertFalse(cache.has(1))


class LastAccessCacheTests(unittest.TestCase, TestBase):
    """
    Tests for L{pyzim.cache.LastAccessCache}.
    """
    def test_cache(self):
        """
        Test caching behavior.
        """
        # test data
        keys_and_values = [
            # (key, value)
            (1, "a"),
            (2, "b"),
            (3, "c"),
            (4, "d"),
            (5, "e"),
            (6, "f"),
        ]
        keys_ordered = [e[0] for e in keys_and_values]
        size = 4

        # setup cache
        cache = LastAccessCache(max_size=size)
        self.assertEqual(cache.max_size, size)

        # ensure cache is empty
        for v in keys_ordered:
            self.assertFalse(cache.has(v))
        # ensure we can not get from an empty cache
        with self.assertRaises(KeyError):
            cache.get(1)
        # push a single element
        # the last access cache will always cache pushed data
        did_cache = cache.push(1, "a")
        self.assertTrue(did_cache)
        self.assertTrue(cache.has(1))
        self.assertFalse(cache.has(2))
        # ensure element was cached
        self.assertEqual(cache.get(1), "a")
        with self.assertRaises(KeyError):
            cache.get(2)
        # ensure clear() works
        cache.clear()
        self.assertFalse(cache.has(1))
        self.assertFalse(cache.has(2))
        with self.assertRaises(KeyError):
            cache.get(1)
        # push all values
        for k, v in keys_and_values:
            did_cache = cache.push(k, v)
            self.assertTrue(did_cache)
        # the last 'size' elements should now be cached
        for k, v in keys_and_values[-size:]:
            self.assertTrue(cache.has(k))
            self.assertEqual(cache.get(k), v)
        for k, v in keys_and_values[:-size]:
            self.assertFalse(cache.has(k))
        # clear and push again, so we have an easier time keeping
        # track of which elements should be in the cache
        cache.clear()
        self.assertFalse(cache.has(1))
        self.assertFalse(cache.has(2))
        with self.assertRaises(KeyError):
            cache.get(1)
        # push all values
        for k, v in keys_and_values:
            did_cache = cache.push(k, v)
            self.assertTrue(did_cache)
        # the last 'size' elements should now be cached
        for k, v in keys_and_values[-size:]:
            self.assertTrue(cache.has(k))
        # pull earliest added element forward
        # forward pulling should happen when an element is accessed
        earliest_cached_kv = keys_and_values[-size]
        second_earliest_cached_kv = keys_and_values[(-size)+1]
        self.assertTrue(cache.has(earliest_cached_kv[0]))
        self.assertEqual(cache.get(earliest_cached_kv[0]), earliest_cached_kv[1])
        # push 3 more elements
        self.assertTrue(cache.push(7, "g"))
        self.assertTrue(cache.push(8, "h"))
        self.assertTrue(cache.push(9, "i"))
        # now, the element pulled forward should still be in cache, but not
        # the second earliest
        self.assertTrue(cache.has(earliest_cached_kv[0]))
        self.assertFalse(cache.has(second_earliest_cached_kv[0]))
        self.assertEqual(cache.get(earliest_cached_kv[0]), earliest_cached_kv[1])

        # clear again
        cache.clear()
        # ensure True when cache gets pushed with the same element twice
        self.assertTrue(cache.push(1, "a"))
        self.assertTrue(cache.push(1, "b"))


class TopAccessCacheTests(unittest.TestCase, TestBase):
    """
    Tests for L{pyzim.cache.TopAccessCache}.
    """
    def test_cache(self):
        """
        Test caching behavior.
        """
        # setup cache
        size = 3
        cache = TopAccessCache(max_size=size)
        # ensure cache is empty
        for i in range(1, 10):
            self.assertFalse(cache.has(i))
        # ensure error when getting non-cached key
        with self.assertRaises(KeyError):
            cache.get(1)
        # push a single element
        self.assertTrue(cache.push(1, "a"))
        self.assertTrue(cache.has(1))
        self.assertEqual(cache.get(1), "a")
        # clear the cache
        cache.clear()
        self.assertFalse(cache.has(1))
        # push an element
        self.assertTrue(query_cache_n_times(cache, 1, "a", 10))
        self.assertTrue(cache.has(1))
        # now push two other elements more
        self.assertTrue(query_cache_n_times(cache, 2, "b", 20))
        self.assertTrue(query_cache_n_times(cache, 3, "c", 30))
        # now all three elements should be in the cache
        self.assertTrue(cache.has(1))
        self.assertEqual(cache.get(1), "a")
        self.assertTrue(cache.has(2))
        self.assertEqual(cache.get(2), "b")
        self.assertTrue(cache.has(3))
        self.assertEqual(cache.get(3), "c")
        # push another element more often than a, pushing it out
        self.assertTrue(query_cache_n_times(cache, 4, "d", 50))
        self.assertTrue(cache.has(4))
        self.assertEqual(cache.get(4), "d")
        self.assertFalse(cache.has(1))
        self.assertTrue(cache.has(2))
        self.assertEqual(cache.get(2), "b")
        self.assertTrue(cache.has(3))
        self.assertEqual(cache.get(3), "c")
        # push another element once, it should not be added to the cache
        self.assertFalse(cache.push(5, "e"))
        self.assertFalse(cache.has(5))
        # push another element more often then d, pushing out b
        self.assertTrue(query_cache_n_times(cache, 6, "f", 70))
        self.assertTrue(cache.has(6))
        self.assertEqual(cache.get(6), "f")
        self.assertFalse(cache.has(1))
        self.assertFalse(cache.has(2))
        self.assertTrue(cache.has(3))
        self.assertEqual(cache.get(3), "c")
        self.assertTrue(cache.has(4))
        self.assertEqual(cache.get(4), "d")
        # push a again 25 times, which should add it again, pushing out c
        self.assertTrue(query_cache_n_times(cache, 1, "a", 25))
        self.assertTrue(cache.has(1))
        self.assertEqual(cache.get(1), "a")
        self.assertTrue(cache.has(6))
        self.assertEqual(cache.get(6), "f")
        self.assertFalse(cache.has(2))
        self.assertTrue(cache.has(4))
        self.assertEqual(cache.get(4), "d")
        # push another element less times than the remaing elements
        self.assertFalse(query_cache_n_times(cache, 7, "g", 30))
        self.assertTrue(cache.has(1))
        self.assertEqual(cache.get(1), "a")
        self.assertTrue(cache.has(6))
        self.assertEqual(cache.get(6), "f")
        self.assertFalse(cache.has(2))
        self.assertTrue(cache.has(4))
        self.assertEqual(cache.get(4), "d")
        self.assertFalse(cache.has(7))

        # clear again
        cache.clear()
        # ensure true on double push
        self.assertTrue(cache.push(8, "h"))
        self.assertTrue(cache.has(8))
        self.assertTrue(cache.push(8, "i"))
        self.assertTrue(cache.has(8))


class HybridCacheTests(unittest.TestCase, TestBase):
    """
    Tests for L{pyzim.cache.HybridCache}.
    """
    def test_cache(self):
        """
        Test caching behavior.
        """
        cache = HybridCache(last_cache_size=1, top_cache_size=1)
        # ensure cache is empty
        for i in range(1, 10):
            self.assertFalse(cache.has(i))
        # ensure error when getting non-cached key
        with self.assertRaises(KeyError):
            cache.get(1)
        # push a single element
        self.assertTrue(cache.push(1, "a"))
        self.assertTrue(cache.has(1))
        self.assertEqual(cache.get(1), "a")
        # clear the cache
        cache.clear()
        self.assertFalse(cache.has(1))
        # add an element multiple times, then another element a single time
        self.assertTrue(query_cache_n_times(cache, 1, "a", 10))
        cache.push(2, "b")
        # both elements should be present
        self.assertTrue(cache.has(1))
        self.assertEqual(cache.get(1), "a")
        self.assertTrue(cache.has(2))
        self.assertEqual(cache.get(2), "b")
        # push a third element a single time, keeping a, but pushing out b
        self.assertTrue(cache.push(3, "c"))
        self.assertTrue(cache.has(1))
        self.assertEqual(cache.get(1), "a")
        self.assertFalse(cache.has(2))
        self.assertTrue(cache.has(3))
        self.assertEqual(cache.get(3), "c")
        # querying c a couple of times, then b, should push a out
        # self.assertTrue(query_cache_n_times(cache, 3, "c", 20))
        did_push = False
        for i in range(20):
            did_push = cache.push(3, "c") or did_push
        self.assertTrue(did_push)
        self.assertTrue(cache.push(2, "b"))
        self.assertFalse(cache.has("a"))
        self.assertTrue(cache.has(2))
        self.assertEqual(cache.get(2), "b")
        self.assertTrue(cache.has(3))
        self.assertEqual(cache.get(3), "c")

        # clear again
        cache.clear()
        # ensure True on double push
        self.assertTrue(cache.push(1, "a"))
        self.assertTrue(cache.push(1, "b"))
