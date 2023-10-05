"""
Tests for L{pyzim.header}.
"""
import unittest

import uuid

from pyzim.header import Header
from pyzim import constants, exceptions

from .base import TestBase


class HeaderTests(unittest.TestCase, TestBase):
    """
    Tests for L{pyzim.header.Header}.
    """
    def read_zts_small(self):
        """
        Read small.zim from the ZTS and return the header.

        @return: the header
        @rtype: L{pyzim.header.Header}
        """
        path = self.get_zts_small_path()
        with open(path, "rb") as fin:
            header = Header.from_file(fin)
        return header

    def test_read_zts_small(self):
        """
        Test that the header from the ZTS small file was parsed correctly.
        """
        header = self.read_zts_small()
        self.assertEqual(header.magic_number, Header.MAGIC_NUMBER)
        self.assertEqual(header.major_version, constants.ZIM_MAJOR_VERSION)
        self.assertEqual(header.minor_version, 1)
        header.check_compatible()
        self.assertGreaterEqual(header.entry_count, 2)
        self.assertLessEqual(header.entry_count, 32)
        self.assertGreaterEqual(header.cluster_count, 1)
        self.assertLessEqual(header.cluster_count, 8)
        self.assertTrue(header.has_main_page)
        self.assertGreaterEqual(header.main_page, 0)
        self.assertIsNotNone(header.main_page)
        self.assertLess(header.main_page, 0xfffffff)  # one f less
        self.assertFalse(header.has_layout_page)
        self.assertIsNone(header.layout_page)

    def test_from_to_bytes(self):
        """
        Test L{pyzim.header.Header.to_bytes} and L{pyzim.header.Header.from_bytes}.
        """
        header = self.read_zts_small()
        dumped = header.to_bytes()
        parsed = header.from_bytes(dumped)
        self.assertDictEqual(header.to_dict(), parsed.to_dict())
        # also assert than from_bytes wont work with incomplete data
        with self.assertRaises(ValueError):
            header.from_bytes(dumped[:-1])

    def test_str(self):
        """
        Test L{pyzim.header.Header.__str__}.
        """
        header = self.read_zts_small()
        s = str(header)
        self.assertIn(str(header.magic_number), s)
        self.assertIn(str(header.major_version) + " (major)", s)
        self.assertIn(str(header.minor_version) + " (minor)", s)
        self.assertIn(str(header.entry_count), s)
        self.assertIn(str(header.cluster_count), s)
        self.assertIn(str(header.cluster_pointer_position), s)
        self.assertIn(str(header.url_pointer_position), s)
        self.assertIn(str(header.mime_list_position), s)
        self.assertIn(str(header.checksum_position), s)
        self.assertIn(str(header.main_page), s)
        self.assertIn("none", s)

    def test_check_compatible(self):
        """
        Test L{pyzim.header.Header.check_compatible}.
        """
        # check that ZTS-small is compatible
        header = self.read_zts_small()
        header.check_compatible()
        # create various variations of the header that are incompatible
        header_wrong_magic = self.read_zts_small()
        header_wrong_magic.magic_number = 127
        with self.assertRaises(exceptions.NotAZimFile):
            header_wrong_magic.check_compatible()

        header_wrong_major = self.read_zts_small()
        header_wrong_major.major_version = 127
        with self.assertRaises(exceptions.IncompatibleZimFile):
            header_wrong_major.check_compatible()

        header_wrong_minor = self.read_zts_small()
        header_wrong_minor.minor_version = 0  # TODO: change when supporting old namespace
        with self.assertRaises(exceptions.IncompatibleZimFile):
            header_wrong_minor.check_compatible()

    def test_placeholder(self):
        """
        Test L{pyzim.header.Header.placeholder}.
        """
        placeholder = Header.placeholder()
        self.assertIsInstance(placeholder, Header)
        self.assertEqual(placeholder.checksum_position, 0)
        self.assertEqual(placeholder.mime_list_position, 0)
        self.assertEqual(placeholder.title_pointer_position, 0)
        self.assertEqual(placeholder.url_pointer_position, 0)
        self.assertEqual(placeholder.cluster_count, 0)
        self.assertEqual(placeholder.entry_count, 0)
        self.assertFalse(placeholder.has_layout_page)
        self.assertIsNone(placeholder.layout_page)
        self.assertFalse(placeholder.has_main_page)
        self.assertIsNone(placeholder.main_page)
        self.assertEqual(placeholder.magic_number, Header.MAGIC_NUMBER)
        self.assertEqual(placeholder.major_version, constants.ZIM_MAJOR_VERSION)
        self.assertEqual(placeholder.minor_version, constants.ZIM_MINOR_VERSION)
        self.assertIsInstance(placeholder.uuid, uuid.UUID)

    def test_properties(self):
        """
        Test the behavior of various properties.
        """
        # list of properties with "regular" behavior
        regular_properties = [
            "checksum_position",
            "cluster_count",
            "cluster_pointer_position",
            "entry_count",
            "major_version",
            "minor_version",
            "mime_list_position",
            "title_pointer_position",
            "url_pointer_position",

        ]
        for attr in regular_properties:
            header = Header.placeholder()
            self.assertTrue(hasattr(header, attr))
            # basic set and get
            setattr(header, attr, 0)
            self.assertEqual(getattr(header, attr), 0)
            header.dirty = False
            # ensure non-mutable works
            header.mutable = False
            with self.assertRaises(exceptions.NonMutable):
                setattr(header, attr, 1)
            self.assertEqual(getattr(header, attr), 0)
            self.assertFalse(header.dirty)
            # ensure changes work when mutable is True again
            header.mutable = True
            setattr(header, attr, 2)
            self.assertEqual(getattr(header, attr), 2)
            self.assertTrue(header.dirty)
            # ensure type checks work
            header.dirty = False
            with self.assertRaises(TypeError):
                setattr(header, attr, "test")
            self.assertEqual(getattr(header, attr), 2)
            self.assertFalse(header.dirty)
            # ensure value checks work
            with self.assertRaises(ValueError):
                setattr(header, attr, -1)
            self.assertFalse(header.dirty)
            self.assertEqual(getattr(header, attr), 2)
            # ensure setting to previous value does not make the header dirty
            setattr(header, attr, 2)
            self.assertFalse(header.dirty)
            self.assertEqual(getattr(header, attr), 2)

        # special property checks
        # uuid conversion
        header = Header.placeholder()
        uuid_1 = uuid.uuid4()
        header.uuid = uuid_1
        self.assertIsInstance(header.uuid, uuid.UUID)
        self.assertEqual(header.uuid, uuid_1)
        self.assertTrue(header.dirty)
        header.dirty = False
        uuid_2 = uuid.uuid4()
        header.uuid = uuid_2.int
        self.assertIsInstance(header.uuid, uuid.UUID)
        self.assertEqual(header.uuid, uuid_2)
        self.assertTrue(header.dirty)
        header.dirty = False
        uuid_3 = uuid.uuid4()
        header.uuid = uuid_3.bytes_le
        self.assertIsInstance(header.uuid, uuid.UUID)
        self.assertEqual(header.uuid, uuid_3)
        self.assertTrue(header.dirty)
        header.dirty = False
        with self.assertRaises(TypeError):
            header.uuid = object()
        self.assertEqual(header.uuid, uuid_3)
        self.assertFalse(header.dirty)
        header.mutable = False
        with self.assertRaises(exceptions.NonMutable):
            header.uuid = uuid_1
        self.assertEqual(header.uuid, uuid_3)
        self.assertFalse(header.dirty)
        header.mutable = True
        header.uuid = uuid_3
        self.assertFalse(header.dirty)

        # magic number (can be negative?)
        header = Header.placeholder()
        header.magic_number = 1
        self.assertEqual(header.magic_number, 1)
        self.assertTrue(header.dirty)
        header.dirty = False
        header.mutable = False
        with self.assertRaises(exceptions.NonMutable):
            header.magic_number = 2
        self.assertEqual(header.magic_number, 1)
        self.assertFalse(header.dirty)
        header.mutable = True
        header.magic_number = 3
        self.assertEqual(header.magic_number, 3)
        self.assertTrue(header.dirty)
        header.dirty = False
        with self.assertRaises(TypeError):
            header.magic_number = "foo"
        self.assertEqual(header.magic_number, 3)
        self.assertFalse(header.dirty)
        header.magic_number = 3
        self.assertFalse(header.dirty)

        # main_page and layout_page
        page_properties = [
            # (attr_name, has_attr_name)
            ("main_page", "has_main_page"),
            ("layout_page", "has_layout_page"),
        ]
        for attr, has_attr in page_properties:
            header = Header.placeholder()
            setattr(header, attr, 1)
            self.assertEqual(getattr(header, attr), 1)
            self.assertTrue(getattr(header, has_attr))
            self.assertTrue(header.dirty)
            header.dirty = False
            setattr(header, attr, None)
            self.assertIsNone(getattr(header, attr))
            self.assertFalse(getattr(header, has_attr))
            self.assertEqual(getattr(header, "_" + attr), 0xffffffff)
            self.assertTrue(header.dirty)
            header.dirty = False
            setattr(header, attr, None)
            self.assertIsNone(getattr(header, attr))
            self.assertFalse(header.dirty)
            header.dirty = False
            setattr(header, attr, 2)
            self.assertEqual(getattr(header, attr), 2)
            self.assertTrue(getattr(header, has_attr))
            self.assertTrue(header.dirty)
            header.dirty = False
            header.mutable = False
            with self.assertRaises(exceptions.NonMutable):
                setattr(header, attr, 3)
            self.assertEqual(getattr(header, attr), 2)
            self.assertTrue(getattr(header, has_attr))
            self.assertFalse(header.dirty)
            header.mutable = True
            with self.assertRaises(TypeError):
                setattr(header, attr, "bar")
            self.assertEqual(getattr(header, attr), 2)
            self.assertTrue(getattr(header, has_attr))
            self.assertFalse(header.dirty)
            setattr(header, attr, 2)
            self.assertEqual(getattr(header, attr), 2)
            self.assertFalse(header.dirty)
            with self.assertRaises(ValueError):
                setattr(header, attr, -1)
            self.assertEqual(getattr(header, attr), 2)
            self.assertFalse(header.dirty)
