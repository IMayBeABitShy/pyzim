"""
Tests for L{pyzim.entry}.
"""
import unittest
from unittest.mock import MagicMock
import io

from pyzim.entry import BaseEntry, RedirectEntry, ContentEntry
from pyzim.blob import InMemoryBlobSource
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

    def test_base_entry_attributes(self):
        """
        Test the various attributes of L{pyzim.entry.BaseEntry}
        """
        with self.open_zts_small_dir() as zimdir:
            with zimdir.open(mode="a") as zim:
                entry = zim.get_mainpage_entry()

                # mimetype_id
                entry.mimetype_id = 1
                entry.dirty = False
                self.assertEqual(entry.mimetype_id, 1)
                with self.assertRaises(TypeError):
                    entry.mimetype_id = "foo"
                with self.assertRaises(ValueError):
                    entry.mimetype_id = -1
                self.assertFalse(entry.dirty)
                # setting value to previous value should not cause dirty state
                entry.mimetype_id = 1
                self.assertFalse(entry.dirty)
                entry.mimetype_id = 2
                self.assertEqual(entry.mimetype_id, 2)
                self.assertTrue(entry.dirty)

                # namespace
                entry.namespace = "C"
                entry.dirty = False
                self.assertEqual(entry.namespace, "C")
                with self.assertRaises(TypeError):
                    entry.namespace = 3
                with self.assertRaises(ValueError):
                    entry.namespace = "foo"
                self.assertFalse(entry.dirty)
                # setting value to previous value should not cause dirty state
                entry.namespace = "C"
                self.assertFalse(entry.dirty)
                entry.namespace = "X"
                self.assertEqual(entry.namespace, "X")
                self.assertTrue(entry.dirty)
                # test bytestring as well
                entry.dirty = False
                entry.namespace = b"T"
                self.assertEqual(entry.namespace, "T")
                self.assertTrue(entry.dirty)

                # revision
                entry.revision = 0
                entry.dirty = False
                self.assertEqual(entry.revision, 0)
                with self.assertRaises(TypeError):
                    entry.revision = "foo"
                with self.assertRaises(ValueError):
                    entry.revision = 7
                self.assertFalse(entry.dirty)
                # setting value to previous value should not cause dirty state
                entry.revision = 0
                self.assertFalse(entry.dirty)
                # TODO: use these tests once the standard allows revision to be set
                # entry.revision = 2
                # self.assertEqual(entry.revision, 2)
                # self.assertTrue(entry.dirty)

                # url
                org_full_url = entry.url
                entry.url = "test/foo/bar"
                entry.dirty = False
                self.assertEqual(entry.url, "test/foo/bar")
                with self.assertRaises(TypeError):
                    entry.url = 3
                with self.assertRaises(ValueError):
                    entry.url = u"foo/bar\x00"
                self.assertFalse(entry.dirty)
                # setting value to previous value should not cause dirty state
                entry.url = "test/foo/bar"
                self.assertFalse(entry.dirty)
                entry.url = "baz/bar"
                self.assertEqual(entry.url, "baz/bar")
                self.assertTrue(entry.dirty)
                # test bytestring as well
                entry.dirty = False
                entry.url = b"bar/foo"
                self.assertEqual(entry.url, "bar/foo")
                self.assertTrue(entry.dirty)
                self.assertEqual(entry.unmodified_full_url[1:], org_full_url)

                # title
                entry.title = "testtitle"
                entry.dirty = False
                self.assertEqual(entry.title, "testtitle")
                with self.assertRaises(TypeError):
                    entry.title = 3
                with self.assertRaises(ValueError):
                    entry.title = u"foo/bar\x00"
                self.assertFalse(entry.dirty)
                # setting value to previous value should not cause dirty state
                entry.title = "testtitle"
                self.assertFalse(entry.dirty)
                entry.title = "title2"
                self.assertEqual(entry.title, "title2")
                self.assertTrue(entry.dirty)
                # test bytestring as well
                entry.dirty = False
                entry.title = b"title3"
                self.assertEqual(entry.title, "title3")
                self.assertTrue(entry.dirty)
                # if title is empty, url should be used
                entry.title = None
                self.assertEqual(entry.title, entry.url)
                entry.title = ""
                self.assertEqual(entry.title, entry.url)

                # parameters
                entry.parameters = []
                entry.dirty = False
                self.assertEqual(entry.parameters, [])
                with self.assertRaises(TypeError):
                    entry.parameters = "test"
                # currently, no parameters are allowed
                # TODO: change this section once this changes
                with self.assertRaises(ValueError):
                    entry.parameters = ["test"]
                self.assertEqual(entry.parameters, [])

                # is_redirect should not be settable
                with self.assertRaises(AttributeError):
                    entry.is_redirect = True
                self.assertFalse(entry.is_redirect)

                # full_url
                self.assertEqual(entry.full_url, entry.namespace + entry.url)
                entry.full_url = "Ttesturl"
                entry.dirty = False
                self.assertEqual(entry.full_url, "Ttesturl")
                with self.assertRaises(TypeError):
                    entry.full_url = ["a", "b"]
                with self.assertRaises(ValueError):
                    entry.full_url = ""
                self.assertEqual(entry.full_url, "Ttesturl")
                self.assertEqual(entry.namespace, "T")
                self.assertEqual(entry.url, "testurl")
                self.assertFalse(entry.dirty)
                entry.dirty = False
                entry.full_url = u"Uunicodeurl"
                self.assertEqual(entry.namespace, "U")
                self.assertEqual(entry.url, "unicodeurl")
                self.assertTrue(entry.dirty)
                # test with bytestring too
                entry.dirty = False
                entry.full_url = b"Bbyteurl"
                self.assertEqual(entry.namespace, "B")
                self.assertEqual(entry.url, "byteurl")
                self.assertTrue(entry.dirty)

                # mimetype
                entry.mimetype_id = 1
                entry.dirty = False
                self.assertEqual(entry.mimetype, zim.get_mimetype_by_index(1))
                entry.mimetype = "testmime"
                self.assertEqual(entry.mimetype, "testmime")
                self.assertTrue(entry.dirty)
                entry.dirty = False
                with self.assertRaises(TypeError):
                    entry.mimetype = 3
                with self.assertRaises(ValueError):
                    entry.mimetype = u"zerotest\x00"
                with self.assertRaises(ValueError):
                    entry.mimetype = ""
                self.assertEqual(entry.mimetype, "testmime")
                self.assertFalse(entry.dirty)
                # setting value to previous value should not cause dirty state
                entry.mimetype = "testmime"
                self.assertFalse(entry.dirty)
                entry.mimetype = "newmime"
                self.assertEqual(entry.mimetype, "newmime")
                self.assertTrue(entry.dirty)
                # test bytestring as well
                entry.dirty = False
                entry.mimetype = b"bytemime"
                self.assertEqual(entry.mimetype, "bytemime")
                self.assertTrue(entry.dirty)
                # set mimetype_id is properly set
                self.assertEqual(entry.mimetype_id, zim.mimetypelist.get_index("bytemime"))
                # check bind required
                entry.unbind()
                with self.assertRaises(exceptions.BindRequired):
                    entry.mimetype = "someothermimetype"
                # bind again for next tests
                entry.bind(zim)

                # is_article
                # get a new entry
                entry = zim.get_mainpage_entry().resolve()
                self.assertTrue(entry.is_article)
                with self.assertRaises(TypeError):
                    entry.is_article = "foobar"
                # check bind required
                entry.unbind()
                with self.assertRaises(exceptions.BindRequired):
                    entry.is_article
                # should work for force set article status
                entry.is_article = True
                entry.is_article
                # bind again for further tests
                entry.bind(zim)
                # entries can not exists outside of "C" namespace
                entry.namespace = "X"
                self.assertFalse(entry.is_article)
                with self.assertRaises(ValueError):
                    entry.is_article = True
                entry.is_article = False  # no exception here
                self.assertTrue(entry.dirty)

    def test_content_entry_attributes(self):
        """
        Test the various attributes of L{pyzim.entry.ContentEntry}
        """
        with self.open_zts_small_dir() as zimdir:
            with zimdir.open(mode="a") as zim:
                entry = zim.get_mainpage_entry().resolve()

                # cluster number
                entry.cluster_number = 5
                self.assertEqual(entry.cluster_number, 5)
                entry.dirty = False
                with self.assertRaises(TypeError):
                    entry.cluster_number = "foobar"
                with self.assertRaises(ValueError):
                    entry.cluster_number = -1
                self.assertEqual(entry.cluster_number, 5)
                entry.cluster_number = 5
                self.assertFalse(entry.dirty)  # because we use the same value
                entry.cluster_number = 3
                self.assertEqual(entry.cluster_number, 3)
                self.assertTrue(entry.dirty)

                # blob number
                entry.blob_number = 5
                self.assertEqual(entry.blob_number, 5)
                entry.dirty = False
                with self.assertRaises(TypeError):
                    entry.blob_number = "foobar"
                with self.assertRaises(ValueError):
                    entry.blob_number = -1
                self.assertEqual(entry.blob_number, 5)
                entry.blob_number = 5
                self.assertFalse(entry.dirty)  # because we use the same value
                entry.blob_number = 3
                self.assertEqual(entry.blob_number, 3)
                self.assertTrue(entry.dirty)

    def test_redirect_entry_attributes(self):
        """
        Test the various attributes of L{pyzim.entry.RedirectEntry}
        """
        with self.open_zts_small_dir() as zimdir:
            with zimdir.open(mode="a") as zim:
                entry = zim.get_mainpage_entry()
                self.assertTrue(entry.is_redirect)

                # redirect_index
                entry.redirect_index = 5
                self.assertEqual(entry.redirect_index, 5)
                entry.dirty = False
                with self.assertRaises(TypeError):
                    entry.redirect_index = "foobar"
                with self.assertRaises(ValueError):
                    entry.redirect_index = -1
                self.assertEqual(entry.redirect_index, 5)
                entry.redirect_index = 5
                self.assertFalse(entry.dirty)  # because we use the same value
                entry.redirect_index = 3
                self.assertEqual(entry.redirect_index, 3)
                self.assertTrue(entry.dirty)

    def test_base_entry_defaults(self):
        """
        Test miscelaneous default implementations in L{pyzim.entry.BaseEntry}.
        """
        with self.open_zts_small() as zim:
            entry = BaseEntry(0, "C", 0, "url", "title", [])
            with self.assertRaises(NotImplementedError):
                entry.get_disk_size()
            with self.assertRaises(NotImplementedError):
                entry.to_bytes()
            # test flush()
            entry.unbind()
            with self.assertRaises(exceptions.BindRequired):
                entry.flush()
            entry.bind(zim)
            zim.write_entry = MagicMock()
            entry.dirty = False
            entry.flush()
            # if not dirty, write_entry should not be called
            zim.write_entry.assert_not_called()
            entry.dirty = True
            entry.flush()
            zim.write_entry.assert_called_with(entry)

    def test_content_entry_serialization(self):
        """
        Test serialization of L{pyzim.entry.ContentEntry}.
        """
        with self.open_zts_small() as zim:
            entry = zim.get_mainpage_entry().resolve()
            expected_size = entry.get_disk_size()
            dumped = entry.to_bytes()
            f = io.BytesIO(dumped)
            loaded = entry.from_file(f)

            self.assertEqual(len(dumped), expected_size)
            self.assertEqual(entry.title, loaded.title)
            self.assertEqual(entry.full_url, loaded.full_url)
            self.assertEqual(entry.mimetype_id, loaded.mimetype_id)
            self.assertEqual(entry.revision, loaded.revision)
            self.assertEqual(entry.parameters, loaded.parameters)
            self.assertEqual(entry.cluster_number, loaded.cluster_number)
            self.assertEqual(entry.blob_number, loaded.blob_number)

    def test_redirect_entry_serialization(self):
        """
        Test serialization of L{pyzim.entry.RedirectEntry}.
        """
        with self.open_zts_small() as zim:
            entry = zim.get_mainpage_entry()
            self.assertTrue(entry.is_redirect)
            expected_size = entry.get_disk_size()
            dumped = entry.to_bytes()
            f = io.BytesIO(dumped)
            loaded = entry.from_file(f)

            self.assertEqual(len(dumped), expected_size)
            self.assertEqual(entry.title, loaded.title)
            self.assertEqual(entry.full_url, loaded.full_url)
            self.assertEqual(entry.mimetype_id, loaded.mimetype_id)
            self.assertEqual(entry.revision, loaded.revision)
            self.assertEqual(entry.parameters, loaded.parameters)
            self.assertEqual(entry.redirect_index, loaded.redirect_index)

    def test_content_entry_remove(self):
        """
        Test L{pyzim.entry.ContentEntry.remove}.
        """
        for blob in ("keep", "remove", "empty"):
            with self.open_zts_small_dir() as zimdir:
                with zimdir.open(mode="u") as zim:
                    entry = zim.get_mainpage_entry().resolve()
                    cluster = entry.get_cluster()
                    old_num_blobs = cluster.get_number_of_blobs()
                    old_data = cluster.read_blob(entry.blob_number)
                    entry.remove(blob=blob)
                    cluster.flush()
                    with self.assertRaises(exceptions.EntryNotFound):
                        zim.get_entry_by_full_url(entry.full_url)
                    if blob == "remove":
                        self.assertEqual(cluster.get_number_of_blobs(), old_num_blobs - 1)
                        self.assertNotEqual(cluster.read_blob(entry.blob_number), old_data)
                    elif blob == "empty":
                        self.assertEqual(cluster.get_number_of_blobs(), old_num_blobs)
                        self.assertEqual(cluster.read_blob(entry.blob_number), b"")
                    elif blob == "keep":
                        self.assertEqual(cluster.get_number_of_blobs(), old_num_blobs)
                        self.assertEqual(cluster.read_blob(entry.blob_number), old_data)
                    else:
                        raise AssertionError("Unreachable state in test!")
                    # test unbound error
                    entry.unbind()
                    with self.assertRaises(exceptions.BindRequired):
                        entry.remove(blob=blob)

    def test_redirect_entry_remove(self):
        """
        Test L{pyzim.entry.RedirectEntry.remove}.
        """
        for blob in ("keep", "remove", "empty"):
            with self.open_zts_small_dir() as zimdir:
                with zimdir.open(mode="u") as zim:
                    entry = zim.get_mainpage_entry()
                    target = entry.resolve()
                    old_data = target.read()
                    entry.remove(blob=blob)
                    with self.assertRaises(exceptions.EntryNotFound):
                        zim.get_entry_by_full_url(entry.full_url)
                    # removing the redirect should not affect target
                    self.assertEqual(zim.get_entry_by_full_url(target.full_url).read(), old_data)
                    # test unbound error
                    entry.unbind()
                    with self.assertRaises(exceptions.BindRequired):
                        entry.remove(blob=blob)

    def test_set_content(self):
        """
        Test l{pyzim.entry.ContentEntry.set_content}.
        """
        with self.open_zts_small_dir() as zimdir:
            with zimdir.open(mode="u") as zim:
                entry = zim.get_mainpage_entry().resolve()
                # test type errors
                with self.assertRaises(TypeError):
                    entry.set_content(1234)
                # test actual run
                # binary data
                data_1 = b"binary data"
                cluster_1 = entry.set_content(data_1)
                cluster_1.flush()
                data_1_copy = zim.get_entry_by_full_url(entry.full_url).read()
                self.assertEqual(data_1_copy, data_1)
                # unicode data
                data_2 = u"Unicode data"
                cluster_2 = entry.set_content(data_2)
                self.assertIs(cluster_2, cluster_1)
                cluster_2.flush()
                data_2_copy = zim.get_entry_by_full_url(entry.full_url).read().decode(constants.ENCODING)
                self.assertEqual(data_2_copy, data_2)
                # blob source
                data_3_raw = b"blob source data"
                data_3 = InMemoryBlobSource(data_3_raw)
                cluster_3 = entry.set_content(data_3)
                self.assertIs(cluster_3, cluster_1)
                cluster_3.flush()
                data_3_copy = zim.get_entry_by_full_url(entry.full_url).read()
                self.assertEqual(data_3_copy, data_3_raw)
                # test unbound error
                entry.unbind()
                with self.assertRaises(exceptions.BindRequired):
                    entry.set_content(b"this should fail")
