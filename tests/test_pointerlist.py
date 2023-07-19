"""
Tests for L{pyzim.pointerlist}.
"""
import io
import struct
import unittest

from pyzim.pointerlist import SimplePointerList, OrderedPointerList
from pyzim import constants, exceptions

from .base import TestBase


class SimplePointerListTests(unittest.TestCase, TestBase):
    """
    Tests for L{pyzim.pointerlist.SimplePointerlist}.
    """
    def setUp(self):
        self.data = [10, 11, 12, 13, 14, 15]
        self.rawlist = struct.pack(constants.ENDIAN + SimplePointerList.POINTER_FORMAT * 6, *self.data)

    def test_simple_read(self):
        """
        Test a simple read operation.
        """
        pointerlist = SimplePointerList.from_bytes(self.rawlist)
        self.assertEqual(len(pointerlist), 6)
        for i in range(len(pointerlist)):
            self.assertEqual(pointerlist.get_by_index(i), i + 10)
        with self.assertRaises(IndexError):
            pointerlist.get_by_index(len(pointerlist))  # out of range

    def test_modify(self):
        """
        Test modification operations.
        """
        pointerlist = SimplePointerList.from_bytes(self.rawlist)
        self.assertEqual(len(pointerlist), 6)
        for i in range(len(pointerlist)):
            self.assertEqual(pointerlist.get_by_index(i), i + 10)
        pointerlist.set(1, 1000)
        self.assertEqual(pointerlist.get_by_index(1), 1000)
        pointerlist.append(2000)
        self.assertEqual(pointerlist.get_by_index(len(pointerlist) - 1), 2000)

    def test_from_file(self):
        """
        Test parsing of a file.
        """
        f = io.BytesIO(b"test" + self.rawlist + b"test")
        pointerlist = SimplePointerList.from_file(f, len(self.data), seek=4)
        self.assertEqual(len(pointerlist), 6)
        for i in range(len(pointerlist)):
            self.assertEqual(pointerlist.get_by_index(i), i + 10)

    def test_to_bytes(self):
        """
        Test L{pyzim.pointerlist.SimplePointerList.to_bytes}.
        """
        pointerlist = SimplePointerList.from_bytes(self.rawlist)
        dumped = pointerlist.to_bytes()
        parsed = SimplePointerList.from_bytes(dumped)
        self.assertListEqual(pointerlist._pointers, parsed._pointers)

    def test_from_bytes_incomplete(self):
        """
        Test L{pyzim.pointerlist.SimplePointerList.from_bytes} with incomplete data.
        """
        with self.assertRaises(ValueError):
            SimplePointerList.from_bytes(self.rawlist[:-1])

    def test_iter_pointers(self):
        """
        Test L{pyzim.pointerlist.SimplePointerList.iter_pointers}.
        """
        pointerlist = SimplePointerList.from_bytes(self.rawlist)
        start_end_values = (
            (0, len(self.data)),
            (0, 1),
            (2, 2),
            (None, None),
            (0, None),
            (None, len(self.data)),
        )
        for start, end in start_end_values:
            # figure out intended values if start and/or end is None
            rl_start = start
            rl_end = end
            if rl_start is None:
                rl_start = 0
            if rl_end is None:
                rl_end = len(self.data)

            pointers = [p for p in pointerlist.iter_pointers(start, end)]
            self.assertEqual(len(pointers), rl_end - rl_start)
            for p_a, p_b in zip(pointers, self.data[rl_start:rl_end]):
                self.assertEqual(p_a, p_b)


class OrderedPointerListTests(unittest.TestCase, TestBase):
    """
    Tests for L{pyzim.pointerlist.OrderedPointerlist}.
    """
    def setUp(self):
        self.data = [b"a", b"b", b"c", b"e", b"f", b"g"]
        self.rawlist = struct.pack(constants.ENDIAN + OrderedPointerList.POINTER_FORMAT * 6, *list(range(6)))

    def keyfunc(self, pointer):
        """
        Utility function that serves as the key function for the pointer list.

        They key in this case is the value in self.data.

        @param pointer: pointer to get key for
        @type pointer: L{int}
        @return: the key for the pointer
        @rtype: L{str}
        """
        assert isinstance(pointer, int)
        return self.data[pointer]

    def test_simple_read(self):
        """
        Test a simple read operation.
        """
        pointerlist = OrderedPointerList.from_bytes(self.rawlist, key_func=self.keyfunc)
        self.assertEqual(len(pointerlist), 6)
        self.assertEqual(pointerlist.get("a"), 0)
        self.assertEqual(pointerlist.get("c"), 2)
        self.assertEqual(pointerlist.get("e"), 3)
        self.assertEqual(pointerlist.get("g"), 5)
        with self.assertRaises(KeyError):
            pointerlist.get("d")
        self.assertTrue(pointerlist.has("a"))
        self.assertTrue(pointerlist.has("b"))
        self.assertFalse(pointerlist.has("d"))

        self.assertEqual(pointerlist.get_by_index(0), 0)
        self.assertEqual(pointerlist.get_by_index(5), 5)
        pointerlist.check_sorted()

    def test_from_file(self):
        """
        Test parsing of a file.
        """
        f = io.BytesIO(b"test" + self.rawlist + b"test")
        pointerlist = OrderedPointerList.from_file(f, len(self.data), key_func=self.keyfunc, seek=4)
        self.assertEqual(len(pointerlist), 6)
        self.assertEqual(pointerlist.get("a"), 0)
        self.assertEqual(pointerlist.get("c"), 2)
        self.assertEqual(pointerlist.get("e"), 3)
        self.assertEqual(pointerlist.get("g"), 5)
        self.assertFalse(pointerlist.has("test"))
        pointerlist.check_sorted()

    def test_add_remove(self):
        """
        Test adding and removing of elements.
        """
        pointerlist = OrderedPointerList.from_bytes(self.rawlist, key_func=self.keyfunc)
        self.assertEqual(pointerlist.get("a"), 0)
        self.assertEqual(pointerlist.get("c"), 2)
        self.assertEqual(pointerlist.get("e"), 3)
        self.assertEqual(pointerlist.get("g"), 5)
        self.assertFalse(pointerlist.has("d"))
        pointerlist.check_sorted()
        pointerlist.add(b"d", 3)
        self.data.insert(3, b"d")
        self.assertEqual(len(pointerlist), 7)
        self.assertTrue(pointerlist.has("d"))
        self.assertEqual(pointerlist.get("d"), 3)
        self.assertEqual(pointerlist.get_by_index(3), 3)
        pointerlist.check_sorted()
        pointerlist.remove(b"d")
        del self.data[3]
        self.assertEqual(len(pointerlist), 6)
        self.assertFalse(pointerlist.has("d"))
        self.assertTrue(pointerlist.has("e"))
        self.assertEqual(pointerlist.get_by_index(3), 3)
        with self.assertRaises(KeyError):
            pointerlist.remove(b"h")
        pointerlist.check_sorted()

    def test_add_remove_unicode(self):
        """
        Test adding and removing of elements with unicode.
        """
        pointerlist = OrderedPointerList.from_bytes(self.rawlist, key_func=self.keyfunc)
        self.assertFalse(pointerlist.has(u"d"))
        self.assertFalse(pointerlist.has(b"d"))
        pointerlist.check_sorted()
        pointerlist.add(u"d", 3)
        self.data.insert(3, b"d")
        self.assertEqual(len(pointerlist), 7)
        self.assertTrue(pointerlist.has(u"d"))
        self.assertTrue(pointerlist.has(b"d"))
        self.assertEqual(pointerlist.get(u"d"), 3)
        self.assertEqual(pointerlist.get(b"d"), 3)
        self.assertEqual(pointerlist.get_by_index(3), 3)
        pointerlist.check_sorted()
        pointerlist.remove(u"d")
        del self.data[3]
        self.assertEqual(len(pointerlist), 6)
        self.assertFalse(pointerlist.has(u"d"))
        self.assertFalse(pointerlist.has(b"d"))
        self.assertTrue(pointerlist.has(u"e"))
        self.assertTrue(pointerlist.has(b"e"))
        self.assertEqual(pointerlist.get_by_index(3), 3)
        with self.assertRaises(KeyError):
            pointerlist.remove(b"h")
        with self.assertRaises(KeyError):
            pointerlist.remove(u"h")
        pointerlist.check_sorted()

    def test_to_bytes(self):
        """
        Test L{pyzim.pointerlist.OrderedPointerList.to_bytes}.
        """
        pointerlist = OrderedPointerList.from_bytes(self.rawlist, key_func=self.keyfunc)
        dumped = pointerlist.to_bytes()
        parsed = OrderedPointerList.from_bytes(dumped, key_func=self.keyfunc)
        self.assertListEqual(pointerlist._pointers, parsed._pointers)

    def test_check_sorted(self):
        """
        Test L{pyzim.pointerlist.OrderedPointerList.check_sorted}.
        """
        sorted_pointerlist = OrderedPointerList.from_bytes(self.rawlist, key_func=self.keyfunc)
        unsorted_data = [b"a", b"b", b"g", b"e", b"f", b"c"]
        unsorted_rawlist = struct.pack(constants.ENDIAN + OrderedPointerList.POINTER_FORMAT * 6, *list(range(6)))
        unsorted_pointerlist = OrderedPointerList.from_bytes(
            unsorted_rawlist,
            key_func=lambda p, d=unsorted_data: d[p],
        )
        # check that check_sorted() on sorted list passes
        sorted_pointerlist.check_sorted()
        # check that check_sorted() on unsorted list does not pass
        with self.assertRaises(exceptions.UnsortedList):
            unsorted_pointerlist.check_sorted()

    def test_from_bytes_incomplete(self):
        """
        Test L{pyzim.pointerlist.OrderedPointerList.from_bytes} with incomplete data.
        """
        with self.assertRaises(ValueError):
            OrderedPointerList.from_bytes(self.rawlist[:-1], key_func=self.keyfunc)

    def test_find_first_greater_equals(self):
        """
        Test L{pyzim.pointerlist.OrderedPointerList.find_first_greater_equals}.
        """
        pointerlist = OrderedPointerList.from_bytes(self.rawlist, key_func=self.keyfunc)
        self.assertEqual(pointerlist.find_first_greater_equals("a"), 0)
        self.assertEqual(pointerlist.find_first_greater_equals("g"), len(self.data) - 1)
        self.assertEqual(pointerlist.find_first_greater_equals("b"), 1)
        self.assertEqual(self.data[pointerlist.find_first_greater_equals("e"):], [b"e", b"f", b"g"])
