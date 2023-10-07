"""
Tests for L{pyzim.util.ioutil}.
"""
import io
import unittest

from pyzim import constants
from pyzim.util.ioutil import read_until_zero, read_n_bytes

from ..base import TestBase


class IoUtilTests(unittest.TestCase, TestBase):
    """
    Tests for L{pyzim.util.ioutil}.
    """
    def test_read_until_zero(self):
        """
        Test L{pyzim.ioutil.read_until_zero}.
        """
        substr_a = b"foo"
        substr_b = b"test"
        data = substr_a + b"\x00" + substr_b + b"\x00bar\x00"
        f = io.BytesIO(data)
        # regular read
        read_a = read_until_zero(f)
        self.assertEqual(read_a, substr_a)
        self.assertIsInstance(read_a, bytes)
        read_b = read_until_zero(f)
        self.assertEqual(read_b, substr_b)
        self.assertIsInstance(read_b, bytes)
        # read with 0
        # NOTE: for some reason f.seek(0) does not work.
        # so instead, we need to create a new instance of BytesIO
        f = io.BytesIO(data)
        read_a = read_until_zero(f, strip_zero=False)
        self.assertEqual(read_a, substr_a + b"\x00")
        self.assertIsInstance(read_a, bytes)
        read_b = read_until_zero(f, strip_zero=False)
        self.assertEqual(read_b, substr_b + b"\x00")
        self.assertIsInstance(read_b, bytes)
        # read explicitly without 0
        f = io.BytesIO(data)
        read_a = read_until_zero(f, strip_zero=True)
        self.assertEqual(read_a, substr_a)
        self.assertIsInstance(read_a, bytes)
        read_b = read_until_zero(f, strip_zero=True)
        self.assertEqual(read_b, substr_b)
        self.assertIsInstance(read_b, bytes)
        # read unicode
        f = io.BytesIO(data)
        read_a = read_until_zero(f, encoding=constants.ENCODING)
        self.assertEqual(read_a, substr_a.decode(constants.ENCODING))
        self.assertIsInstance(read_a, str)
        read_b = read_until_zero(f, encoding=constants.ENCODING)
        self.assertEqual(read_b, substr_b.decode(constants.ENCODING))
        self.assertIsInstance(read_b, str)
        # read unicode with 0
        f = io.BytesIO(data)
        read_a = read_until_zero(f, strip_zero=False, encoding=constants.ENCODING)
        self.assertEqual(read_a[:-1], substr_a.decode(constants.ENCODING))
        self.assertIsInstance(read_a, str)
        read_b = read_until_zero(f, strip_zero=False, encoding=constants.ENCODING)
        self.assertEqual(read_b[:-1], substr_b.decode(constants.ENCODING))
        self.assertIsInstance(read_b, str)

    def test_read_n_bytes(self):
        """
        Test L{pyzim.ioutil.read_n_bytes}.
        """
        # TODO: modify file-like object so multiple reads are needed
        data = b"test"
        f = io.BytesIO(data)
        read_a = read_n_bytes(f, 2)
        self.assertEqual(read_a, data[:2])
        read_b = read_n_bytes(f, 2)
        self.assertEqual(read_b, data[2:])
        f.seek(0)
        read_c = read_n_bytes(f, 1024)
        self.assertEqual(read_c, data)
        # check raise on incomplete behavior
        f.seek(0)
        with self.assertRaises(IOError):
            read_n_bytes(f, 1024, raise_on_incomplete=True)
        f.seek(0)
        self.assertEqual(read_n_bytes(f, 1024, raise_on_incomplete=False), b"test")
