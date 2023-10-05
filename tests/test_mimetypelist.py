"""
Tests for L{pyzim.mimetypelist}.
"""
import io
import unittest

from pyzim import exceptions
from pyzim.constants import ENCODING
from pyzim.mimetypelist import MimeTypeList
from pyzim.header import Header

from .base import TestBase


class MimeTypeListTests(unittest.TestCase, TestBase):
    """
    Tests for L{pyzim.mimetypelist.MimeTypeList}.
    """
    def test_simple_read(self):
        """
        Test a simple read-only use.
        """
        mimetypes = MimeTypeList([b"foo", b"bar", b"baz"])
        self.assertEqual(len(mimetypes), 3)
        self.assertTrue(mimetypes.has("foo"))
        self.assertTrue(mimetypes.has("bar"))
        self.assertTrue(mimetypes.has("baz"))
        self.assertFalse(mimetypes.has("abc"))
        self.assertEqual(mimetypes.get(0), b"foo")
        self.assertEqual(mimetypes.get(0, as_unicode=True), u"foo")
        self.assertEqual(mimetypes.get_index("bar"), 1)
        self.assertFalse(mimetypes.dirty)
        # ensure nothing was changed
        self.assertEqual(len(mimetypes), 3)
        # assert IndexError on invalid index
        with self.assertRaises(IndexError):
            mimetypes.get(-1)
        with self.assertRaises(IndexError):
            mimetypes.get(3)
        # ensure still not dirty
        self.assertFalse(mimetypes.dirty)

    def test_parse(self):
        """
        Test parsing of a MIME type list.
        """
        data = b"testfoo\x00bar\x00baz\x00\x00test"
        f = io.BytesIO(data)
        mimetypes = MimeTypeList.from_file(f, seek=4)
        self.assertEqual(len(mimetypes), 3)
        self.assertTrue(mimetypes.has("foo"))
        self.assertTrue(mimetypes.has("bar"))
        self.assertTrue(mimetypes.has("baz"))
        self.assertFalse(mimetypes.has("abc"))
        self.assertEqual(mimetypes.get(0), b"foo")
        self.assertFalse(mimetypes.dirty)

    def test_to_bytes(self):
        """
        Test L{pyzim.mimetypelist.MimeTypeList.to_bytes}.
        """
        mimetypes = MimeTypeList([b"foo", b"bar", b"baz"])
        dumped = mimetypes.to_bytes()
        self.assertEqual(len(dumped), mimetypes.get_disk_size())
        parsed = MimeTypeList.from_file(io.BytesIO(dumped))
        self.assertListEqual(mimetypes._mimetypes, parsed._mimetypes)

    def test_get_index(self):
        """
        Test L{pyzim.mimetypelist.MimeTypeList.get_index}.
        """
        mimetypes = MimeTypeList([b"foo", b"bar", b"baz"])
        self.assertEqual(len(mimetypes), 3)
        self.assertEqual(mimetypes.get_index("foo"), 0)
        self.assertEqual(mimetypes.get_index("bar"), 1)
        self.assertEqual(mimetypes.get_index("baz"), 2)
        self.assertIsNone(mimetypes.get_index("test"))
        self.assertFalse(mimetypes.dirty)
        # check that it was not registered
        self.assertIsNone(mimetypes.get_index("test"))
        # now register it
        self.assertEqual(mimetypes.get_index("test", register=True), 3)
        self.assertEqual(len(mimetypes), 4)
        self.assertTrue(mimetypes.dirty)
        mimetypes.dirty = False
        # check that it will not be added again
        self.assertEqual(mimetypes.get_index("test", register=True), 3)
        self.assertEqual(len(mimetypes), 4)
        self.assertFalse(mimetypes.dirty)
        self.assertEqual(mimetypes.get_index("test", register=False), 3)
        self.assertEqual(len(mimetypes), 4)
        self.assertFalse(mimetypes.dirty)

    def test_register(self):
        """
        Test L{pyzim.mimetypelist.MimeTypeList.register}.
        """
        mimetypes = MimeTypeList([b"foo", b"bar", b"baz"])
        self.assertEqual(len(mimetypes), 3)
        self.assertFalse(mimetypes.dirty)
        # register an already existing one
        mimetypes.register(b"foo")
        self.assertEqual(len(mimetypes), 3)
        self.assertTrue(mimetypes.has("foo"))
        self.assertFalse(mimetypes.dirty)
        # register a new one
        mimetypes.register(b"test")
        self.assertEqual(len(mimetypes), 4)
        self.assertTrue(mimetypes.has("test"))
        self.assertTrue(mimetypes.dirty)
        mimetypes.dirty = False
        # check that unicode/bytes mismatch will not lead to duplicates
        mimetypes.register(u"test")
        self.assertEqual(len(mimetypes), 4)
        self.assertTrue(mimetypes.has("foo"))
        self.assertFalse(mimetypes.dirty)
        # register new unicode
        mimetypes.register(u"test2")
        self.assertEqual(len(mimetypes), 5)
        self.assertTrue(mimetypes.has("test2"))
        self.assertTrue(mimetypes.dirty)
        # ensure all initial mimetypes still remain
        self.assertTrue(mimetypes.has("foo"))
        self.assertTrue(mimetypes.has("bar"))
        self.assertTrue(mimetypes.has("baz"))
        # ensure failure on non-mutable
        mimetypes.dirty = False
        mimetypes.mutable = False
        with self.assertRaises(exceptions.NonMutable):
            mimetypes.register("test3")
        self.assertTrue(mimetypes.has("foo"))
        self.assertTrue(mimetypes.has("bar"))
        self.assertTrue(mimetypes.has("baz"))
        self.assertFalse(mimetypes.has("test3"))
        self.assertFalse(mimetypes.dirty)

    def test_iter_mimetypes(self):
        """
        Test L{pyzim.mimetypelist.MimeTypeList.iter_mimetypes}.
        """
        mimetypes_raw = [b"foo", b"bar", b"baz"]
        mimetypes = MimeTypeList(mimetypes_raw)
        self.assertEqual(len(mimetypes), 3)
        for org, mimetype in zip(mimetypes_raw, mimetypes.iter_mimetypes()):
            self.assertEqual(org, mimetype)
        for org, mimetype in zip(mimetypes_raw, mimetypes.iter_mimetypes(as_unicode=True)):
            self.assertEqual(org.decode(ENCODING), mimetype)
        self.assertFalse(mimetypes.dirty)

    def test_zts_small(self):
        """
        Test the correct parsing of small.zim from the ZTS.
        """
        path = self.get_zts_small_path()
        with open(path, "rb") as fin:
            header = Header.from_file(fin)
            mtl = MimeTypeList.from_file(fin, seek=header.mime_list_position)
            self.assertGreater(len(mtl), 2)
            self.assertLess(len(mtl), 10)
            self.assertTrue(mtl.has("text/html"))
            self.assertTrue(mtl.has("image/png"))
            self.assertFalse(mtl.dirty)

    def test_str(self):
        """
        Test L{pyzim.mimetypelist.MimeTypeList.__str__}.
        """
        mimetypes_raw = [b"a", b"bc", b"def"]
        mimetypes = MimeTypeList(mimetypes_raw)
        s = str(mimetypes)
        for mt in mimetypes_raw:
            self.assertIn(mt.decode(ENCODING) + "\n", s)
        self.assertFalse(mimetypes.dirty)

    def test_get_disk_size(self):
        """
        Test L{pyzim.mimetypelist.MimeTypeList.get_disk_size}.
        """
        testdata = [
            # (mimetypelist, expected length).
            ([], 1),
            ([b"foo"], 5),
            ([b"foo", b"bar"], 9),
            ([u"täst".encode("utf-8")], len(u"täst".encode("utf-8"))+2),
        ]
        for mimetypes_raw, expected in testdata:
            mimetypes = MimeTypeList(mimetypes_raw)
            self.assertEqual(mimetypes.get_disk_size(), expected)
