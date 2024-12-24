"""
Tests for L{pyzim.pointerlist}.
"""
import io
import struct
import unittest
from unittest import mock

from pyzim.pointerlist import SimplePointerList, OrderedPointerList, TitlePointerList
from pyzim.pointerlist import OnDiskSimplePointerList, OnDiskOrderedPointerList, OnDiskTitlePointerList
from pyzim import constants, exceptions

from .base import TestBase


class SimplePointerListTests(unittest.TestCase, TestBase):
    """
    Tests for L{pyzim.pointerlist.SimplePointerlist}.
    """
    def setUp(self):
        self.data = [10, 11, 12, 13, 14, 15]
        self.rawlist = struct.pack(constants.ENDIAN + SimplePointerList.POINTER_FORMAT * 6, *self.data)

    def test_new(self):
        """
        Test L{pyzim.pointerlist.SimplePointerList.new}.
        """
        pointerlist = SimplePointerList.new()
        self.assertEqual(len(pointerlist), 0)
        self.assertEqual(pointerlist._pointers, [])

    def test_simple_read(self):
        """
        Test a simple read operation.
        """
        pointerlist = SimplePointerList.from_bytes(self.rawlist)
        self.assertEqual(len(pointerlist), 6)
        self.assertFalse(pointerlist.dirty)
        for i in range(len(pointerlist)):
            self.assertEqual(pointerlist.get_by_index(i), i + 10)
        with self.assertRaises(IndexError):
            pointerlist.get_by_index(len(pointerlist))  # out of range
        self.assertFalse(pointerlist.dirty)

    def test_from_file(self):
        """
        Test parsing of a file.
        """
        f = io.BytesIO(b"test" + self.rawlist + b"test")
        pointerlist = SimplePointerList.from_file(f, len(self.data), seek=4)
        self.assertEqual(len(pointerlist), 6)
        for i in range(len(pointerlist)):
            self.assertEqual(pointerlist.get_by_index(i), i + 10)
        # repeat without seek parameter
        f.seek(4)
        pointerlist = SimplePointerList.from_file(f, len(self.data))
        self.assertEqual(len(pointerlist), 6)
        for i in range(len(pointerlist)):
            self.assertEqual(pointerlist.get_by_index(i), i + 10)

    def test_from_zim_entry(self):
        """
        Test L{pyzim.pointerlist.SimplePointerList.from_zim_entry}.
        """
        with self.open_zts_small() as zim:
            # we've got a bit of a problem here: there's no simple
            # pointer list stored inside an entry of any ZIM file,
            # only title pointer lists. To test this anyway, we are going
            # to overwrite the pointer format for this test
            with mock.patch("pyzim.pointerlist.SimplePointerList.POINTER_FORMAT", TitlePointerList.POINTER_FORMAT):
                pointerlist = SimplePointerList.from_zim_entry(zim, constants.URL_ENTRY_TITLE_INDEX)
                pointers = list(pointerlist.iter_pointers())
                orgpointers = list(zim._entry_title_pointer_list.iter_pointers())
                self.assertEqual(pointers, orgpointers)

    def test_to_bytes(self):
        """
        Test L{pyzim.pointerlist.SimplePointerList.to_bytes}.
        """
        pointerlist = SimplePointerList.from_bytes(self.rawlist)
        dumped = pointerlist.to_bytes()
        self.assertEqual(len(dumped), pointerlist.get_disk_size())
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

    def test_get_by_pointer(self):
        """
        Test L{pyzim.pointerlist.SimplePointerList.get_by_pointer}.
        """
        pointers = [10, 20, 30, 40, 50]
        pointerlist = SimplePointerList(pointers)
        self.assertEqual(pointerlist.get_by_pointer(10), 0)
        self.assertEqual(pointerlist.get_by_pointer(30), 2)
        self.assertEqual(pointerlist.get_by_pointer(50), 4)
        with self.assertRaises(KeyError):
            pointerlist.get_by_pointer(35)

    def test_modify(self):
        """
        Test modification operations.
        """
        pointerlist = SimplePointerList.from_bytes(self.rawlist)
        self.assertEqual(len(pointerlist), 6)
        self.assertFalse(pointerlist.dirty)
        for i in range(len(pointerlist)):
            self.assertEqual(pointerlist.get_by_index(i), i + 10)
        pointerlist.set(1, 1000)
        self.assertEqual(pointerlist.get_by_index(1), 1000)
        self.assertTrue(pointerlist.dirty)
        pointerlist.dirty = False
        pointerlist.append(2000)
        self.assertEqual(pointerlist.get_by_index(len(pointerlist) - 1), 2000)
        self.assertTrue(pointerlist.dirty)
        # test set() with index >= len(pointerlist)
        pointerlist.dirty = False
        # i == len(pointerlist) should append value
        pointerlist.set(len(pointerlist), 10000)
        self.assertEqual(len(pointerlist), 8)
        self.assertEqual(pointerlist.get_by_index(7), 10000)
        self.assertTrue(pointerlist.dirty)
        # i > len with add_placeholders=False should raise an exception
        pointerlist.dirty = False
        with self.assertRaises(IndexError):
            pointerlist.set(12, 11000, add_placeholders=False)
        self.assertEqual(len(pointerlist), 8)
        self.assertEqual(pointerlist.get_by_index(7), 10000)
        self.assertFalse(pointerlist.dirty)
        # i > len with add_placeholders=True should add placeholders
        pointerlist.dirty = False
        pointerlist.set(12, 11000, add_placeholders=True)
        self.assertEqual(len(pointerlist), 13)
        self.assertEqual(pointerlist.get_by_index(10), 11000)
        self.assertEqual(pointerlist.get_by_index(12), 11000)
        self.assertTrue(pointerlist.dirty)
        # ensure an error will be raised if not mutable
        pointerlist.mutable = False
        pointerlist.dirty = False
        with self.assertRaises(exceptions.NonMutable):
            pointerlist.append(3000)
        self.assertFalse(pointerlist.dirty)
        self.assertEqual(len(pointerlist), 13)
        with self.assertRaises(exceptions.NonMutable):
            pointerlist.set(1, 500)
        self.assertFalse(pointerlist.dirty)
        self.assertEqual(pointerlist.get_by_index(1), 1000)

    def test_remove_by_index(self):
        """
        Test L{pyzim.pointerlist.SimplePointerList.remove_by_index}.
        """
        pointers = [10, 20, 30, 40, 50]
        pointerlist = SimplePointerList(pointers)
        pointerlist.remove_by_index(0)
        self.assertEqual(len(pointerlist), 4)
        self.assertEqual(pointerlist.get_by_index(0), 20)
        pointerlist.remove_by_index(1)
        self.assertEqual(len(pointerlist), 3)
        self.assertEqual(pointerlist.get_by_index(1), 40)
        pointerlist.remove_by_index(2)
        self.assertEqual(len(pointerlist), 2)
        self.assertEqual(pointerlist.get_by_index(1), 40)
        # check that this now also changes the last accessible index
        with self.assertRaises(IndexError):
            pointerlist.get_by_index(2)

    def test_mass_update(self):
        """
        Test L{pyzim.pointerlist.SimplePointerList.mass_update}.
        """
        pointers = [10, 20, 30, 40, 50]
        pointerlist = SimplePointerList(pointers)
        # update with 0 should not cause any changes
        pointerlist.mass_update(0)
        self.assertEqual(list(pointerlist.iter_pointers()), pointers)
        # increment by 1
        pointerlist.mass_update(1)
        self.assertEqual(
            list(pointerlist.iter_pointers()),
            [11, 21, 31, 41, 51],
        )
        # decrement by 1
        pointerlist.mass_update(-1)
        self.assertEqual(list(pointerlist.iter_pointers()), pointers)
        # ensure pointerlist is marked as dirty due to the modifications
        self.assertTrue(pointerlist.dirty)

        # check with specified ranges
        # NOTE: ranges refers to pointer values
        # end=0 should cause no modifications
        pointerlist.mass_update(1, end=0)
        self.assertEqual(list(pointerlist.iter_pointers()), pointers)
        # same with start=51
        pointerlist.mass_update(1, start=51)
        self.assertEqual(list(pointerlist.iter_pointers()), pointers)
        # change first two values
        pointerlist.mass_update(1, end=21)
        self.assertEqual(
            list(pointerlist.iter_pointers()),
            [11, 21, 30, 40, 50],
        )
        # change last two values
        pointerlist.mass_update(-2, start=40)
        self.assertEqual(
            list(pointerlist.iter_pointers()),
            [11, 21, 30, 38, 48],
        )
        # change middle 3 values
        pointerlist.mass_update(5, start=21, end=39)
        self.assertEqual(
            list(pointerlist.iter_pointers()),
            [11, 26, 35, 43, 48],
        )

    def test_get_disk_size(self):
        """
        Test L{pyzim.pointerlist.SimplePointerList.get_disk_size}.
        """
        testdata = [
            # ([pointers], expected size),
            ([], 0),
            ([1], 8),
            ([1, 2,], 16),
            ([1, 2, 3], 24),
        ]
        for pointers, expected in testdata:
            pointerlist = SimplePointerList(pointers)
            self.assertEqual(pointerlist.get_disk_size(), expected)
            self.assertEqual(pointerlist.get_disk_size(), len(pointerlist.to_bytes()))


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
        self.assertFalse(pointerlist.dirty)

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
        # repeat without seek parameter
        f = io.BytesIO(b"test" + self.rawlist + b"test")
        f.seek(4)
        pointerlist = OrderedPointerList.from_file(f, len(self.data), key_func=self.keyfunc)
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
        self.assertFalse(pointerlist.dirty)
        pointerlist.add(b"d", 3)
        self.data.insert(3, b"d")
        self.assertEqual(len(pointerlist), 7)
        self.assertTrue(pointerlist.has("d"))
        self.assertEqual(pointerlist.get("d"), 3)
        self.assertEqual(pointerlist.get_by_index(3), 3)
        self.assertTrue(pointerlist.dirty)
        pointerlist.dirty = False
        pointerlist.check_sorted()
        pointerlist.remove(b"d")
        del self.data[3]
        self.assertEqual(len(pointerlist), 6)
        self.assertFalse(pointerlist.has("d"))
        self.assertTrue(pointerlist.has("e"))
        self.assertEqual(pointerlist.get_by_index(3), 3)
        self.assertTrue(pointerlist.dirty)
        pointerlist.dirty = False
        with self.assertRaises(KeyError):
            pointerlist.remove(b"h")
        self.assertFalse(pointerlist.dirty)
        pointerlist.check_sorted()
        # ensure an error is raised when the list is not mutable
        pointerlist.mutable = False
        with self.assertRaises(exceptions.NonMutable):
            pointerlist.add("z", 27)
        self.assertFalse(pointerlist.dirty)
        pointerlist.check_sorted()
        self.assertFalse(pointerlist.has("z"))
        with self.assertRaises(exceptions.NonMutable):
            pointerlist.remove("a")
        self.assertFalse(pointerlist.dirty)
        pointerlist.check_sorted()
        self.assertTrue(pointerlist.has("a"))

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
        self.assertEqual(pointerlist.find_first_greater_equals(b"a"), 0)
        self.assertEqual(pointerlist.find_first_greater_equals("g"), len(self.data) - 1)
        self.assertEqual(pointerlist.find_first_greater_equals("b"), 1)
        self.assertEqual(self.data[pointerlist.find_first_greater_equals("e"):], [b"e", b"f", b"g"])

    def test_iter_values(self):
        """
        Test L{pyzim.pointerlist.OrderedPointerList.iter_values}.
        """
        pointerlist = OrderedPointerList.from_bytes(self.rawlist, key_func=self.keyfunc)
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

            values = [v for v in pointerlist.iter_values(start, end)]
            self.assertEqual(len(values), rl_end - rl_start)
            for v_a, v_b in zip(values, self.data[rl_start:rl_end]):
                self.assertEqual(v_a, v_b)


class OnDiskSimplePointerListTests(unittest.TestCase, TestBase):
    """
    Tests for L{pyzim.pointerlist.OnDiskSimplePointerlist}.
    """
    def test_raise_unsupported_init(self):
        """
        Test that various initializers not supported by L{pyzim.pointerlist.OnDiskSimplePointerlist} raise the appropiate error.
        """
        with self.open_zts_small() as zim:
            with self.assertRaises(exceptions.OperationNotSupported):
                OnDiskSimplePointerList.new()
            with self.assertRaises(exceptions.OperationNotSupported):
                OnDiskSimplePointerList.from_bytes(b"")
            with self.assertRaises(exceptions.OperationNotSupported):
                with zim.acquire_file() as f:
                    OnDiskSimplePointerList.from_file(f, zim.header.cluster_count, seek=zim.header.cluster_pointer_position)

    def test_from_zim_file(self):
        """
        Test L{pyzim.pointerlist.OnDiskSimplePointerList.from_zim_file}
        """
        # attempt to manually locate a cluster for the main page entry
        with self.open_zts_small() as zim:
            entry = zim.get_mainpage_entry().resolve()
            cluster = entry.get_cluster()
            pointerlist = OnDiskSimplePointerList.from_zim_file(zim, n=zim.header.cluster_count, seek=zim.header.cluster_pointer_position)
            self.assertEqual(pointerlist.get_by_index(entry.cluster_number), cluster.offset)
        # repeat without seek parameter
        with self.open_zts_small() as zim:
            entry = zim.get_mainpage_entry().resolve()
            cluster = entry.get_cluster()
            with zim.acquire_file() as f:
                f.seek(zim.header.cluster_pointer_position)
            pointerlist = OnDiskSimplePointerList.from_zim_file(zim, n=zim.header.cluster_count)
            self.assertEqual(pointerlist.get_by_index(entry.cluster_number), cluster.offset)

    def test_from_zim_entry(self):
        """
        Test L{pyzim.pointerlist.OnDiskSimplePointerList.from_zim_entry}
        """
        with self.open_zts_small() as zim:
            # we've got a bit of a problem here: there's no simple
            # pointer list stored inside an entry of any ZIM file,
            # only title pointer lists. To test this anyway, we are going
            # to overwrite the pointer format for this test
            with mock.patch("pyzim.pointerlist.OnDiskSimplePointerList.POINTER_FORMAT", TitlePointerList.POINTER_FORMAT):
                pointerlist = OnDiskSimplePointerList.from_zim_entry(zim, constants.URL_ENTRY_TITLE_INDEX)
                pointers = list(pointerlist.iter_pointers())
                orgpointers = list(zim._entry_title_pointer_list.iter_pointers())
                self.assertEqual(pointers, orgpointers)


class OnDiskOrderedPointerListTests(unittest.TestCase, TestBase):
    """
    Tests for L{pyzim.pointerlist.OnDiskOrderedPointerlist}.
    """
    def test_raise_unsupported_init(self):
        """
        Test that various initializers not supported by L{pyzim.pointerlist.OnDiskOrderedPointerlist} raise the appropiate error.
        """
        with self.open_zts_small() as zim:
            with self.assertRaises(exceptions.OperationNotSupported):
                OnDiskOrderedPointerList.new(key_func=lambda x: 0)
            with self.assertRaises(exceptions.OperationNotSupported):
                OnDiskOrderedPointerList.from_bytes(b"", key_func=lambda x: 0)
            with self.assertRaises(exceptions.OperationNotSupported):
                with zim.acquire_file() as f:
                    OnDiskOrderedPointerList.from_file(f, zim.header.entry_count, seek=zim.header.cluster_pointer_position, key_func=lambda x: 0)

    def test_from_zim_file(self):
        """
        Test L{pyzim.pointerlist.OnDiskOrderedPointerList.from_zim_file}
        """
        # attempt to locate main entry
        with self.open_zts_small() as zim:
            pointerlist = OnDiskOrderedPointerList.from_zim_file(
                zim,
                n=zim.header.entry_count,
                seek=zim.header.url_pointer_position,
                key_func=zim._get_full_url_for_entry_at,
            )
            self.assertEqual(len(pointerlist), zim.header.entry_count)
            pointerlist.check_sorted()
        # repeat without seek parameter
        with self.open_zts_small() as zim:
            with zim.acquire_file() as f:
                f.seek(zim.header.url_pointer_position)
            pointerlist = OnDiskOrderedPointerList.from_zim_file(
                zim,
                n=zim.header.entry_count,
                key_func=zim._get_full_url_for_entry_at,
            )
            self.assertEqual(len(pointerlist), zim.header.entry_count)
            pointerlist.check_sorted()

    def test_from_zim_entry(self):
        """
        Test L{pyzim.pointerlist.OnDiskOrderedPointerList.from_zim_entry}
        """
        with self.open_zts_small() as zim:
            pointerlist = OnDiskTitlePointerList.from_zim_entry(
                zim,
                constants.URL_ENTRY_TITLE_INDEX,
                key_func=zim._get_namespace_title_for_entry_by_url_index,
            )
            pointers = list(pointerlist.iter_pointers())
            orgpointers = list(zim._entry_title_pointer_list.iter_pointers())
            self.assertEqual(pointers, orgpointers)
            pointerlist.check_sorted()
