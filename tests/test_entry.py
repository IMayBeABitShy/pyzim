"""
Tests for L{pyzim.entry}.
"""
import unittest

from pyzim.entry import BaseEntry, RedirectEntry, ContentEntry
from pyzim import constants, exceptions

from .base import TestBase


class EntryTests(unittest.TestCase, TestBase):
    """
    Tests for L{pyzim.entry.BaseEntry} and its subclasses.
    """
    def test_info_mainpage(self):
        """
        Test info about the mainpage
        """
        with self.open_zts_small() as zim:
            entry_redirect = zim.get_mainpage_entry()
            self.assertEqual(entry_redirect.namespace, "W")
            self.assertTrue(entry_redirect.is_redirect)
            self.assertEqual(entry_redirect.url, "mainPage")
            self.assertEqual(entry_redirect.title.strip(), "mainPage")
            self.assertEqual(entry_redirect.mimetype, constants.MIMETYPE_REDIRECT)
            self.assertEqual(entry_redirect.revision, 0)

            entry = entry_redirect.resolve()

            self.assertEqual(entry.namespace, "C")
            self.assertFalse(entry.is_redirect)
            self.assertEqual(entry.url, "main.html")
            self.assertEqual(entry.title, "Test ZIM file")
            self.assertEqual(entry.mimetype, "text/html")
            self.assertEqual(entry.revision, 0)

    def test_unbound(self):
        """
        Test behavior of an unbound entry.
        """
        with self.open_zts_small() as zim:
            # it's easier to just load the main entry and unbind it afterwards
            entry_redirect = zim.get_mainpage_entry()
            entry = entry_redirect.resolve()
            entry_redirect.unbind()
            entry.unbind()
            self.assertFalse(entry.bound)
            self.assertFalse(entry_redirect.bound)

            # content entry tests
            with self.assertRaises(exceptions.BindRequired):
                entry.mimetype
            with self.assertRaises(exceptions.BindRequired):
                entry.get_cluster()
            with self.assertRaises(exceptions.BindRequired):
                entry.get_size()
            with self.assertRaises(exceptions.BindRequired):
                entry.read()
            with self.assertRaises(exceptions.BindRequired):
                for c in entry.iter_read():
                    pass

            # redirect entry tests
            with self.assertRaises(exceptions.BindRequired):
                entry_redirect.follow()

    def test_get_size(self):
        """
        Test L{pyzim.entry.ContentEntry.get_size}.
        """
        with self.open_zts_small() as zim:
            entry_redirect = zim.get_mainpage_entry()
            entry = entry_redirect.resolve()
            size = entry.get_size()
            self.assertGreater(size, 0)
            self.assertLess(size, 1024)

    def test_read(self):
        """
        Test L{pyzim.entry.ContentEntry.read}.
        """
        with self.open_zts_small() as zim:
            entry_redirect = zim.get_mainpage_entry()
            entry = entry_redirect.resolve()
            content = entry.read()
            self.assertIn(b"<html>", content)
            self.assertIn(b"<title>Test ZIM file</title>", content)
            self.assertIn(b"</html>", content)

    def test_iter_read(self):
        """
        Test L{pyzim.entry.ContentEntry.iter_read}.
        """
        with self.open_zts_small() as zim:
            entry_redirect = zim.get_mainpage_entry()
            entry = entry_redirect.resolve()
            content = entry.read()
            iter_content = b"".join([c for c in entry.iter_read(buffersize=3)])
            self.assertEqual(iter_content, content)

    def test_base_entry_from_file(self):
        """
        Test L{pyzim.entry.BaseEntry.from_file}.
        """
        with self.open_zts_small() as zim:
            entry_redirect = zim.get_mainpage_entry()
            entry_0 = entry_redirect.resolve()
            entry_location = zim._url_pointer_list.get_by_index(entry_redirect.redirect_index)
            with zim.acquire_file() as f:
                entry_1 = BaseEntry.from_file(f, seek=entry_location)
            # bind so we can access mimetype, ...
            entry_1.bind(zim)
            # check that entry 0 and 1
            # we do not need to perform a full check, that is done elsewhere
            self.assertEqual(entry_0.mimetype_id, entry_1.mimetype_id)
            self.assertEqual(entry_0.mimetype, entry_1.mimetype)
            self.assertEqual(entry_0.namespace, entry_1.namespace)
            self.assertEqual(entry_0.url, entry_1.url)
            self.assertEqual(entry_0.title, entry_1.title)

    def test_content_entry_from_file(self):
        """
        Test L{pyzim.entry.ContentEntry.from_file}.
        """
        # normally, this method would be called from BaseEntry.from_file().
        # here, we want to to test specific parameters (e.g. mimetype
        # omitted), thus we call it manually.
        with self.open_zts_small() as zim:
            entry_redirect = zim.get_mainpage_entry()
            entry_0 = entry_redirect.resolve()
            entry_location = zim._url_pointer_list.get_by_index(entry_redirect.redirect_index)
            with zim.acquire_file() as f:
                entry_1 = ContentEntry.from_file(f, seek=entry_location)
                entry_2 = ContentEntry.from_file(f, seek=entry_location+2, mimetype=entry_0.mimetype_id)
            # check that entry 0, 1 and 2 match
            # we do not need to perform a full check, that is done elsewhere
            self.assertEqual(entry_0.mimetype_id, entry_1.mimetype_id)
            self.assertEqual(entry_0.namespace, entry_1.namespace)
            self.assertEqual(entry_0.url, entry_1.url)
            self.assertEqual(entry_0.title, entry_1.title)
            self.assertEqual(entry_0.mimetype_id, entry_2.mimetype_id)
            self.assertEqual(entry_0.namespace, entry_2.namespace)
            self.assertEqual(entry_0.url, entry_2.url)
            self.assertEqual(entry_0.title, entry_2.title)

    def test_redirect_entry_from_file(self):
        """
        Test L{pyzim.entry.RedirectEntry.from_file}.
        """
        # normally, this method would be called from BaseEntry.from_file().
        # here, we want to to test specific parameters (e.g. mimetype
        # omitted), thus we call it manually.
        with self.open_zts_small() as zim:
            entry_0 = zim.get_mainpage_entry()
            entry_location = zim._url_pointer_list.get_by_index(zim.header.main_page)
            with zim.acquire_file() as f:
                entry_1 = RedirectEntry.from_file(f, seek=entry_location)
                entry_2 = RedirectEntry.from_file(f, seek=entry_location+2, mimetype=0xffff)
            # check that entry 0, 1 and 2 match
            # we do not need to perform a full check, that is done elsewhere
            self.assertEqual(entry_0.mimetype_id, entry_1.mimetype_id)
            self.assertEqual(entry_0.namespace, entry_1.namespace)
            self.assertEqual(entry_0.redirect_index, entry_1.redirect_index)
            self.assertEqual(entry_0.url, entry_1.url)
            self.assertEqual(entry_0.title, entry_1.title)
            self.assertEqual(entry_0.mimetype_id, entry_2.mimetype_id)
            self.assertEqual(entry_0.namespace, entry_2.namespace)
            self.assertEqual(entry_0.redirect_index, entry_2.redirect_index)
            self.assertEqual(entry_0.url, entry_2.url)
            self.assertEqual(entry_0.title, entry_2.title)
