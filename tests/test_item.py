"""
Tests for L{pyzim.item}.
"""
import unittest

from pyzim.item import Item
from pyzim.entry import ContentEntry
from pyzim.blob import InMemoryBlobSource

from .base import TestBase


class ItemTests(unittest.TestCase, TestBase):
    """
    Tests for L{pyzim.item.Item}.
    """
    def test_attributes(self):
        """
        Test attribute getting/setting and __init__.
        """
        # start item
        namespace = "C"
        url = "test.html"
        mimetype = "application/testdata"
        blob_source = InMemoryBlobSource("test")
        title = "The Test"

        item = Item(
            namespace=namespace,
            url=url,
            mimetype=mimetype,
            blob_source=blob_source,
            title=title,
            is_article=True,
        )

        # ensure all attributes were correctly set
        self.assertEqual(item.namespace, namespace)
        self.assertEqual(item.url, url)
        self.assertEqual(item.mimetype, mimetype)
        self.assertIs(item.blob_source, blob_source)
        self.assertEqual(item.title, title)
        self.assertTrue(item.is_article)

        # test attribute modification
        # namespace
        with self.assertRaises(TypeError):
            item.namespace = b"T"
        with self.assertRaises(ValueError):
            item.namespace = "test"
        self.assertEqual(item.namespace, namespace)
        item.namespace = "F"
        self.assertEqual(item.namespace, "F")
        # url
        with self.assertRaises(TypeError):
            item.url = b"test"
        with self.assertRaises(ValueError):
            item.url = ""
        with self.assertRaises(ValueError):
            item.url = u"test\x00"
        self.assertEqual(item.url, url)
        item.url = "foobar"
        self.assertEqual(item.url, "foobar")
        # mimetype
        with self.assertRaises(TypeError):
            item.mimetype = b"test"
        with self.assertRaises(ValueError):
            item.mimetype = ""
        with self.assertRaises(ValueError):
            item.mimetype = u"test\x00"
        self.assertEqual(item.mimetype, mimetype)
        item.mimetype = "baz"
        self.assertEqual(item.mimetype, "baz")
        # blob source
        with self.assertRaises(TypeError):
            item.blob_source = b"bytestring"
        self.assertIs(item.blob_source, blob_source)
        new_blob_source = InMemoryBlobSource(b"other bytestring")
        item.blob_source = new_blob_source
        self.assertIs(item.blob_source, new_blob_source)
        # title
        with self.assertRaises(TypeError):
            item.title = b"test"
        with self.assertRaises(ValueError):
            item.title = u"test\x00"
        self.assertEqual(item.title, title)
        # setting title to None should use the url instead
        item.title = None
        self.assertEqual(item.title, item.url)
        item.title = "barbaz"
        self.assertEqual(item.title, "barbaz")
        # is_article
        with self.assertRaises(TypeError):
            item.is_article = "foo"
        self.assertTrue(item.is_article)
        item.is_article = False
        self.assertFalse(item.is_article)

    def test_to_entry(self):
        """
        Test L{pyzim.item.Item.to_entry}.
        """
        with self.open_zts_small_dir() as zimdir:
            with zimdir.open(mode="a") as zim:
                namespace = "C"
                url = "test.html"
                mimetype = "application/testdata"
                data = b"testdata"
                blob_source = InMemoryBlobSource(data)
                title = "The Test"

                item = Item(
                    namespace=namespace,
                    url=url,
                    mimetype=mimetype,
                    blob_source=blob_source,
                    title=title,
                    is_article=True,
                )

                entry = item.to_entry(zim=zim)
                self.assertIsInstance(entry, ContentEntry)
                self.assertEqual(entry.namespace, namespace)
                self.assertEqual(entry.url, url)
                self.assertEqual(entry.mimetype, mimetype)
                self.assertEqual(entry.title, title)
                self.assertTrue(entry.is_article)

    def test_from_entry(self):
        """
        Test L{pyzim.entry.from_entry}.
        """
        # test type error
        with self.assertRaises(TypeError):
            Item.from_entry(3)
        # test from_entry
        with self.open_zts_small() as zim:
            entry = zim.get_mainpage_entry().resolve()
            item = Item.from_entry(entry)
            self.assertEqual(item.namespace, entry.namespace)
            self.assertEqual(item.url, entry.url)
            self.assertEqual(item.mimetype, entry.mimetype)
            self.assertEqual(item.title, entry.title)
            self.assertEqual(item.is_article, entry.is_article)
            self.assertEqual(item.blob_source.get_blob().read(4096), entry.read())
