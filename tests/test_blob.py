"""
Tests for L{pyzim.blob}.
"""
import tempfile
import os

import unittest

from pyzim.blob import BaseBlobSource, BaseBlob, InMemoryBlobSource, FileBlobSource, EmptyBlobSource, EntryBlobSource, EntryBlob
from pyzim.constants import ENCODING

from .base import TestBase


class HelperBlobSource(BaseBlobSource):
    """
    A helper blob source creating L{TestBlob}.
    """
    def get_blob(self):
        return HelperBlob()


class HelperBlob(BaseBlob):
    """
    A helper blob.
    """
    STRING = b"test"

    def __init__(self):
        BaseBlob.__init__(self)
        self._did_read = False

    def get_size(self):
        return len(self.STRING)

    def read(self, n):
        if self._did_read:
            return b""
        else:
            self._did_read = True
            return self.STRING


class BlobTests(unittest.TestCase, TestBase):
    """
    Tests for L{pyzim.blob}.
    """
    def test_blobsource_get_size(self):
        """
        Test the L{pyzim.blob.BaseBlobSource.get_size} default implementation.
        """
        source = HelperBlobSource()
        size = source.get_size()
        blob = source.get_blob()
        self.assertEqual(size, len(blob.read(1024)))

    def test_blobsource_default(self):
        """
        Test miscelaneous default implementations in L{pyzim.blob.BaseBlobSource}.
        """
        source = BaseBlobSource()
        with self.assertRaises(NotImplementedError):
            source.get_blob()

    def test_blob_default(self):
        """
        Test miscelaneous default implementations in L{pyzim.blob.BaseBlob}.
        """
        blob = BaseBlob()
        with self.assertRaises(NotImplementedError):
            blob.get_size()
        with self.assertRaises(NotImplementedError):
            blob.read(1024)
        # close should not raise a NotImplementedError by default
        blob.close()

    def test_inmemory(self):
        """
        Test L{pyzim.blob.InMemoryBlobSource} and related classes.
        """
        bs = b"test"
        source = InMemoryBlobSource(bs)
        self.assertEqual(source.get_size(), len(bs))
        # test that two blobs don't interfere with each other
        blob_1 = source.get_blob()
        blob_2 = source.get_blob()
        self.assertEqual(blob_1.get_size(), len(bs))
        self.assertEqual(blob_1.read(2), bs[:2])
        self.assertEqual(blob_2.read(3), bs[:3])
        # ensure length still correct
        self.assertEqual(blob_1.get_size(), len(bs))
        self.assertEqual(blob_1.read(1024), bs[2:])
        blob_1.close()
        self.assertEqual(blob_2.read(1024), bs[3:])

        # repeat with unicode
        us = u"test"
        bs = us.encode(ENCODING)
        source = InMemoryBlobSource(us)
        self.assertEqual(source.get_size(), len(bs))
        # test that two blobs don't interfere with each other
        blob_1 = source.get_blob()
        blob_2 = source.get_blob()
        self.assertEqual(blob_1.get_size(), len(bs))
        self.assertEqual(blob_1.read(2), bs[:2])
        self.assertEqual(blob_2.read(3), bs[:3])
        # ensure length still correct
        self.assertEqual(blob_1.get_size(), len(bs))
        self.assertEqual(blob_1.read(1024), bs[2:])
        blob_1.close()
        self.assertEqual(blob_2.read(1024), bs[3:])

        # repeat with unicode and encoding
        encoding = "utf-16"
        us = u"test"
        bs = us.encode(encoding)
        source = InMemoryBlobSource(us, encoding=encoding)
        self.assertEqual(source.get_size(), len(bs))
        # test that two blobs don't interfere with each other
        blob_1 = source.get_blob()
        blob_2 = source.get_blob()
        self.assertEqual(blob_1.get_size(), len(bs))
        self.assertEqual(blob_1.read(2), bs[:2])
        self.assertEqual(blob_2.read(3), bs[:3])
        # ensure length still correct
        self.assertEqual(blob_1.get_size(), len(bs))
        self.assertEqual(blob_1.read(1024), bs[2:])
        blob_1.close()
        self.assertEqual(blob_2.read(1024), bs[3:])

    def test_file(self):
        """
        Test L{pyzim.blob.FileBlobSource} and related classes.
        """
        with tempfile.TemporaryDirectory() as tempdir:
            fp = os.path.join(tempdir, "test.txt")
            bs = b"test"
            with open(fp, "wb") as fout:
                fout.write(bs)
            source = FileBlobSource(fp)
            self.assertEqual(source.get_size(), 4)
            # test that two blobs don't interfere with each other
            blob_1 = source.get_blob()
            blob_2 = source.get_blob()
            self.assertEqual(blob_1.get_size(), len(bs))
            self.assertEqual(blob_1.read(2), bs[:2])
            self.assertEqual(blob_2.read(3), bs[:3])
            # ensure length still correct
            self.assertEqual(blob_1.get_size(), len(bs))
            self.assertEqual(blob_1.read(1024), bs[2:])
            blob_1.close()
            self.assertEqual(blob_2.read(1024), bs[3:])

    def test_empty(self):
        """
        Test L{pyzim.blob.EmptyBlobSource}.
        """
        source = EmptyBlobSource()
        self.assertEqual(source.get_size(), 0)
        blob = source.get_blob()
        self.assertEqual(blob.get_size(), 0)
        data = blob.read(4096)
        self.assertTrue(isinstance(data, bytes))
        self.assertEqual(data, b"")
        blob.close()

    def test_entry(self):
        """
        Test L{pyzim.blob.EntryBlobSource}.
        """
        with self.open_zts_small() as zim:
            mainpage = zim.get_mainpage_entry().resolve()
            data = mainpage.read()
            source = EntryBlobSource(mainpage)
            self.assertEqual(source.get_size(), mainpage.get_size())
            blob = source.get_blob()
            self.assertEqual(blob.get_size(), mainpage.get_size())
            read_data = b""
            chunk = True
            while chunk:
                chunk = blob.read(4096)
                read_data += chunk
            self.assertEqual(read_data, data)
            self.assertEqual(len(read_data), mainpage.get_size())
            blob.close()
            # also test EntryBlob without size specification
            mainpage = zim.get_mainpage_entry().resolve()
            data = mainpage.read()
            blob = EntryBlob(mainpage)
            self.assertEqual(blob.get_size(), mainpage.get_size())
            read_data = b""
            chunk = True
            while chunk:
                chunk = blob.read(4096)
                read_data += chunk
            self.assertEqual(read_data, data)
            self.assertEqual(len(read_data), mainpage.get_size())
            blob.close()
