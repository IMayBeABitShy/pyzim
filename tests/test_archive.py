"""
Tests for L{pyzim.header}.
"""
import unittest

import pytest

from pyzim import Zim, exceptions, policy, constants

from .base import TestBase


@pytest.mark.parametrize(
    "policy",
    policy.ALL_POLICIES,
)
class ZimReaderTests(unittest.TestCase, TestBase):
    """
    Tests for L{pyzim.archive.Zim} in reading mode.
    """

    def test_open(self):
        """
        Test L{pyzim.archive.Zim.open} on ZTS-small.
        """
        zim_policy = policy.DEFAULT_POLICY  # TODO: parameterize
        zim = Zim.open(self.get_zts_small_path(), policy=zim_policy)
        self.assertFalse(zim.closed)
        zim.header.check_compatible()
        self.assertTrue(zim.header.has_main_page)
        self.assertGreater(zim.header.entry_count, 0)
        self.assertGreater(zim.header.cluster_count, 0)
        zim.close()
        self.assertTrue(zim.closed)

        # test error on opening in write mode
        # NOTE: this will work in the future, so remove this then
        with self.assertRaises(NotImplementedError):
            zim.open("temp.zim", "w")

    def test_context(self):
        """
        Test L{pyzim.archive.Zim} as a context manager.
        """
        with Zim.open(self.get_zts_small_path()) as zim:
            self.assertFalse(zim.closed)
            zim.header.check_compatible()
            self.assertTrue(zim.header.has_main_page)
            self.assertGreater(zim.header.entry_count, 0)
            self.assertGreater(zim.header.cluster_count, 0)
        self.assertTrue(zim.closed)

    def test_get_entry_at(self):
        """
        Test L{pyzim.archive.Zim.get_entry_at}.
        """
        with self.open_zts_small() as zim:
            mainpage_pos = zim._url_pointer_list.get_by_index(zim.header.main_page)
            entry = zim.get_entry_at(mainpage_pos).resolve()
            self.assertIn(b"Test ZIM file", entry.read())

    def test_get_entry_by_url(self):
        """
        Test L{pyzim.archive.Zim.get_by_url}.
        """
        with self.open_zts_small() as zim:
            entry = zim.get_entry_by_url("C", "main.html").resolve()
            self.assertIn(b"Test ZIM file", entry.read())
            with self.assertRaises(exceptions.EntryNotFound):
                zim.get_entry_by_url("M", "main.html")
            with self.assertRaises(exceptions.EntryNotFound):
                zim.get_entry_by_url("C", "nonexistent.html")

    def test_get_entry_by_full_url(self):
        """
        Test L{pyzim.archive.Zim.get_by_full_url}.
        """
        with self.open_zts_small() as zim:
            entry = zim.get_entry_by_full_url("Cmain.html").resolve()
            self.assertIn(b"Test ZIM file", entry.read())
            with self.assertRaises(exceptions.EntryNotFound):
                zim.get_entry_by_full_url("Mmain.html")
            with self.assertRaises(exceptions.EntryNotFound):
                zim.get_entry_by_full_url("Cnonexistent.html")

    def test_get_content_entry_by_url(self):
        """
        Test L{pyzim.archive.Zim.get_content_entry_by_url}.
        """
        with self.open_zts_small() as zim:
            entry = zim.get_content_entry_by_url("main.html").resolve()
            self.assertIn(b"Test ZIM file", entry.read())
            with self.assertRaises(exceptions.EntryNotFound):
                zim.get_content_entry_by_url("nonexistent.html")

    def test_get_entry_by_url_index(self):
        """
        Test L{pyzim.archive.Zim.get_by_url_index}.
        """
        with self.open_zts_small() as zim:
            entry = zim.get_entry_by_url_index(zim.header.main_page).resolve()
            self.assertIn(b"Test ZIM file", entry.read())
            with self.assertRaises(exceptions.EntryNotFound):
                zim.get_entry_by_url_index(-1)
            with self.assertRaises(exceptions.EntryNotFound):
                zim.get_entry_by_url_index(zim.header.entry_count * 2)

    def test_get_mainpage_entry(self):
        """
        Test L{pyzim.archive.Zim.get_by_url}..
        """
        with self.open_zts_small() as zim:
            entry = zim.get_mainpage_entry().resolve()
            self.assertIn(b"Test ZIM file", entry.read())
            # modify header so that it does not have a mainpage
            zim.header.main_page = 0xffffffff
            with self.assertRaises(exceptions.EntryNotFound):
                zim.get_mainpage_entry()

    def test_iter_entries(self):
        """
        Test L{pyzim.archive.Zim.iter_entries}.
        """
        # Test strategy:
        # ensure we never see the same URL twice
        # ensure we've seen the main page
        # ensure that the number of total iterations equals the contained entries.
        # don't bother with start and end, as those are passed to pointerlist
        # and tested there
        urls_seen = []
        has_seen_mainpage = False
        with self.open_zts_small() as zim:
            for entry in zim.iter_entries():
                url = entry.url
                self.assertNotIn(url, urls_seen)
                urls_seen.append(url)
                if not entry.is_redirect:
                    content = entry.read()
                    is_mainpage = b"Test ZIM file" in content
                    has_seen_mainpage = has_seen_mainpage or is_mainpage
            self.assertEqual(len(urls_seen), zim.header.entry_count)
            self.assertTrue(has_seen_mainpage)

    def test_iter_articles(self):
        """
        Test L{pyzim.archive.Zim.iter_articles}.
        """
        # Test strategy:
        # ensure we never see the same URL twice
        # ensure we've seen the main page
        # ensure that the number of total iterations equals the contained articles.
        # don't bother with start and end, as those are passed to pointerlist
        # and tested there

        with self.open_zts_small() as zim:
            # figure out number of articles
            num_articles = len(zim._article_title_pointer_list)

            urls_seen = []
            has_seen_mainpage = False
            for entry in zim.iter_articles():
                self.assertEqual(entry.namespace, "C")
                url = entry.url
                self.assertNotIn(url, urls_seen)
                urls_seen.append(url)
                if not entry.is_redirect:
                    content = entry.read()
                    is_mainpage = b"Test ZIM file" in content
                    has_seen_mainpage = has_seen_mainpage or is_mainpage
            self.assertEqual(len(urls_seen), num_articles)
            self.assertTrue(has_seen_mainpage)

    def test_iter_mimetypes(self):
        """
        Test L{pyzim.archive.Zim.iter_mimetypes}.
        """
        with self.open_zts_small() as zim:
            # figure out actual number of mimetypes
            seen_mimetypes = []
            for entry in zim.iter_entries():
                if entry.is_redirect:
                    continue
                mt = entry.mimetype
                if mt not in seen_mimetypes:
                    seen_mimetypes.append(mt)
            num_mimetypes = len(seen_mimetypes)
            # binary strings
            mimetypes = []
            for mt in zim.iter_mimetypes(as_unicode=False):
                self.assertIsInstance(mt, bytes)
                self.assertNotIn(mt, mimetypes)
                mimetypes.append(mt)
            self.assertEqual(len(mimetypes), num_mimetypes)

        # unicode strings
        mimetypes = []
        with self.open_zts_small() as zim:
            for mt in zim.iter_mimetypes(as_unicode=True):
                self.assertIsInstance(mt, str)
                self.assertNotIn(mt, mimetypes)
                mimetypes.append(mt)
            self.assertEqual(len(mimetypes), num_mimetypes)

    def test_iter_clusters(self):
        """
        Test L{pyzim.archive.Zim.iter_clusters}.
        """
        with self.open_zts_small() as zim:
            # check raise on invalid start, end
            with self.assertRaises(IndexError):
                for cluster in zim.iter_clusters(start=-1):
                    pass
            with self.assertRaises(IndexError):
                for cluster in zim.iter_clusters(end=zim.header.cluster_count + 1):
                    pass
            with self.assertRaises(IndexError):
                for cluster in zim.iter_clusters(start=1, end=0):
                    pass

            # check iterating once for every cluster
            offsets = []
            for cluster in zim.iter_clusters():
                self.assertNotIn(cluster.offset, offsets)
                offsets.append(cluster.offset)
                # also check that infobyte can be parsed
                # if the offset is wrong this *may* fail, hinting at a bug
                cluster.read_infobyte_if_needed()
            self.assertEqual(len(offsets), zim.header.cluster_count)

            # check with valid start, end
            # unfortunately, I doubt small.zim contains enough clusters for
            # an extensive tests
            offsets = []
            for cluster in zim.iter_clusters(start=1, end=2):
                self.assertNotIn(cluster.offset, offsets)
                offsets.append(cluster.offset)
                # also check that infobyte can be parsed
                # if the offset is wrong this *may* fail, hinting at a bug
                cluster.read_infobyte_if_needed()
            self.assertEqual(len(offsets), 1)

    def test_pointer_lists(self):
        """
        Test (some of) the pointer lists.
        """
        with self.open_zts_small() as zim:
            zim._url_pointer_list.check_sorted()
            zim._entry_title_pointer_list.check_sorted()
            zim._article_title_pointer_list.check_sorted()

    def test_get_cluster_index_by_offset(self):
        """
        Test L{pyzim.archive.Zim.get_cluster_index_by_offset}.
        """
        with self.open_zts_small() as zim:
            cluster_1 = zim.get_cluster_by_index(1)
            with self.assertRaises(KeyError):
                zim.get_cluster_index_by_offset(cluster_1.offset + 7)
            with self.assertRaises(KeyError):
                zim.get_cluster_index_by_offset(0)
            with self.assertRaises(KeyError):
                zim.get_cluster_index_by_offset(cluster_1.offset - 7)

            self.assertEqual(zim.get_cluster_index_by_offset(cluster_1.offset), 1)

    def test_get_metadata(self):
        """
        Test L{pyzim.archive.Zim.get_metadata} and related functions.
        """
        with self.open_zts_small() as zim:
            self.assertEqual(zim.get_metadata("Title", as_unicode=False), b"Test ZIM file")
            self.assertEqual(zim.get_metadata("Title", as_unicode=True), u"Test ZIM file")
            self.assertIsNone(zim.get_metadata("_test", as_unicode=False))
            self.assertIsNone(zim.get_metadata("_test", as_unicode=True))

            # check metadata dict with unicode
            metadata = zim.get_metadata_dict(as_unicode=True)
            keys = list(sorted(metadata.keys()))
            expected_keys = [
                "Counter",
                "Creator",
                "Date",
                "Description",
                "Illustration_48x48@1",
                "Language",
                "Publisher",
                "Scraper",
                "Tags",
                "Title",
            ]
            self.assertEqual(keys, expected_keys)
            self.assertIsInstance(metadata["Scraper"], str)
            self.assertIsInstance(metadata["Illustration_48x48@1"], bytes)  # always bytes
            self.assertEqual(metadata["Language"], "en")

            # check metadata dict with binary data
            metadata = zim.get_metadata_dict(as_unicode=False)
            keys = list(sorted(metadata.keys()))
            expected_keys = [
                b"Counter",
                b"Creator",
                b"Date",
                b"Description",
                b"Illustration_48x48@1",
                b"Language",
                b"Publisher",
                b"Scraper",
                b"Tags",
                b"Title",
            ]
            self.assertEqual(keys, expected_keys)
            self.assertIsInstance(metadata[b"Scraper"], bytes)
            self.assertIsInstance(metadata[b"Illustration_48x48@1"], bytes)
            self.assertEqual(metadata[b"Language"], b"en")

    def test_checksum(self):
        """
        Test L{pyzim.archive.Zim.get_checksum} and L{pyzim.archive.Zim.calculate_checksum}.
        """
        with self.open_zts_small() as zim:
            read_checksum = zim.get_checksum()
            self.assertIsInstance(read_checksum, bytes)
            self.assertEqual(len(read_checksum), constants.CHECKSUM_LENGTH)
            calc_checksum = zim.calculate_checksum()
            self.assertIsInstance(calc_checksum, bytes)
            self.assertEqual(len(calc_checksum), constants.CHECKSUM_LENGTH)
            self.assertEqual(read_checksum, calc_checksum)
