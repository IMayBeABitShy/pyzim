"""
Tests for L{pyzim.header}.
"""
import unittest

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
        self.assertLess(header.main_page, 0xfffffff)  # one f less
        self.assertFalse(header.has_layout_page)
        self.assertEqual(header.layout_page, 0xffffffff)

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
