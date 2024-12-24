"""
Tests for L{pyzim.header}.

@var logger: logger used by some of these testa
@type logger: L{pyzim.logging.Logger}
"""
import unittest
import logging

from pyzim import Zim, exceptions, policy, constants, item, blob, cache, cluster
from pyzim.compression import CompressionType

from .base import TestBase


logger = logging.getLogger("pyzim.tests.archive")


class ZimReaderTests(unittest.TestCase, TestBase):
    """
    Tests for L{pyzim.archive.Zim} in reading mode.
    """

    policy = policy.DEFAULT_POLICY

    def test_open(self):
        """
        Test L{pyzim.archive.Zim.open} on ZTS-small.
        """
        zim = Zim.open(self.get_zts_small_path(), policy=self.policy)
        self.assertFalse(zim.closed)
        zim.header.check_compatible()
        self.assertTrue(zim.header.has_main_page)
        self.assertGreater(zim.header.entry_count, 0)
        self.assertGreater(zim.header.cluster_count, 0)
        zim._check_closed()
        zim.close()
        self.assertTrue(zim.closed)
        with self.assertRaises(exceptions.ZimFileClosed):
            zim._check_closed()
        # also test non-recognized mode
        with self.assertRaises(ValueError):
            Zim.open("test.zim", mode="z")

    def test_constructor_invalid_values(self):
        """
        Test L{pyzim.archive.Zim.__init__} with an invalid values.
        """
        # offset
        with self.assertRaises(TypeError):
            Zim(None, offset="string")
        with self.assertRaises(ValueError):
            Zim(None, offset=-5)
        # mode
        with self.assertRaises(TypeError):
            Zim(None, mode=3)
        with self.assertRaises(ValueError):
            Zim(None, mode="z")
        # policy
        with self.assertRaises(TypeError):
            Zim(None, policy="test")

    def test_context(self):
        """
        Test L{pyzim.archive.Zim} as a context manager.
        """
        with Zim.open(self.get_zts_small_path(), policy=self.policy) as zim:
            self.assertFalse(zim.closed)
            zim.header.check_compatible()
            self.assertTrue(zim.header.has_main_page)
            self.assertGreater(zim.header.entry_count, 0)
            self.assertGreater(zim.header.cluster_count, 0)
        self.assertTrue(zim.closed)
        # test with exception
        with self.assertRaises(Exception):
            with Zim.open(self.get_zts_small_path()) as zim:
                self.assertFalse(zim.closed)
                raise Exception("Test exception")
        self.assertTrue(zim.closed)

    def test_get_entry_at(self):
        """
        Test L{pyzim.archive.Zim.get_entry_at}.
        """
        with self.open_zts_small(policy=self.policy) as zim:
            mainpage_pos = zim._url_pointer_list.get_by_index(zim.header.main_page)
            entry = zim.get_entry_at(mainpage_pos).resolve()
            self.assertIn(b"Test ZIM file", entry.read())

    def test_get_entry_by_url(self):
        """
        Test L{pyzim.archive.Zim.get_by_url}.
        """
        with self.open_zts_small(policy=self.policy) as zim:
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
        with self.open_zts_small(policy=self.policy) as zim:
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
        with self.open_zts_small(policy=self.policy) as zim:
            entry = zim.get_content_entry_by_url("main.html").resolve()
            self.assertIn(b"Test ZIM file", entry.read())
            with self.assertRaises(exceptions.EntryNotFound):
                zim.get_content_entry_by_url("nonexistent.html")

    def test_get_entry_by_url_index(self):
        """
        Test L{pyzim.archive.Zim.get_by_url_index}.
        """
        with self.open_zts_small(policy=self.policy) as zim:
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
        # for this test, we need to be capable of modifying the header
        # so we work on a copy of ZTS-small
        with self.open_zts_small_dir() as zimdir:
            with zimdir.open(mode="u", policy=self.policy) as zim:
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
        with self.open_zts_small(policy=self.policy) as zim:
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

        with self.open_zts_small(policy=self.policy) as zim:
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
        with self.open_zts_small(policy=self.policy) as zim:
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
        with self.open_zts_small(policy=self.policy) as zim:
            for mt in zim.iter_mimetypes(as_unicode=True):
                self.assertIsInstance(mt, str)
                self.assertNotIn(mt, mimetypes)
                mimetypes.append(mt)
            self.assertEqual(len(mimetypes), num_mimetypes)

    def test_iter_clusters(self):
        """
        Test L{pyzim.archive.Zim.iter_clusters}.
        """
        with self.open_zts_small(policy=self.policy) as zim:
            # check raise on invalid start, end
            with self.assertRaises(IndexError):
                for zimcluster in zim.iter_clusters(start=-1):
                    pass
            with self.assertRaises(IndexError):
                for zimcluster in zim.iter_clusters(end=zim.header.cluster_count + 1):
                    pass
            with self.assertRaises(IndexError):
                for zimcluster in zim.iter_clusters(start=1, end=0):
                    pass

            # check iterating once for every cluster
            offsets = []
            for zimcluster in zim.iter_clusters():
                self.assertNotIn(zimcluster.offset, offsets)
                offsets.append(zimcluster.offset)
                # also check that infobyte can be parsed
                # if the offset is wrong this *may* fail, hinting at a bug
                zimcluster.read_infobyte_if_needed()
            self.assertEqual(len(offsets), zim.header.cluster_count)

            # check with valid start, end
            # unfortunately, I doubt small.zim contains enough clusters for
            # an extensive tests
            offsets = []
            for zimcluster in zim.iter_clusters(start=1, end=2):
                self.assertNotIn(zimcluster.offset, offsets)
                offsets.append(zimcluster.offset)
                # also check that infobyte can be parsed
                # if the offset is wrong this *may* fail, hinting at a bug
                zimcluster.read_infobyte_if_needed()
            self.assertEqual(len(offsets), 1)

    def test_pointer_lists(self):
        """
        Test (some of) the pointer lists.
        """
        with self.open_zts_small(policy=self.policy) as zim:
            zim._url_pointer_list.check_sorted()
            zim._entry_title_pointer_list.check_sorted()
            zim._article_title_pointer_list.check_sorted()

    def test_get_cluster_index_by_offset(self):
        """
        Test L{pyzim.archive.Zim.get_cluster_index_by_offset}.
        """
        with self.open_zts_small(policy=self.policy) as zim:
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
        with self.open_zts_small(policy=self.policy) as zim:
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
        with self.open_zts_small(policy=self.policy) as zim:
            read_checksum = zim.get_checksum()
            self.assertIsInstance(read_checksum, bytes)
            self.assertEqual(len(read_checksum), constants.CHECKSUM_LENGTH)
            calc_checksum = zim.calculate_checksum()
            self.assertIsInstance(calc_checksum, bytes)
            self.assertEqual(len(calc_checksum), constants.CHECKSUM_LENGTH)
            self.assertEqual(read_checksum, calc_checksum)


class ZimWriterTests(unittest.TestCase, TestBase):
    """
    Tests for L{pyzim.archive.Zim} in writing/update mode.
    """

    policy = policy.DEFAULT_POLICY

    def test_meta_zimcheck_warning(self):
        """
        A meta-test, that will skip itself if zimcheck is not present.

        This will inform the user that some tests have been skipped, so
        the user knows that zimcheck is not present.
        """
        if not self.has_zimcheck():
            self.skipTest("Zimcheck is not present!")

    def test_add_item(self):
        """
        Test L{pyzim.archive.Zim.add_item}.
        """
        with self.open_temp_dir() as zimdir:
            with zimdir.open(mode="w", policy=self.policy) as zim:
                # check invalid values
                with self.assertRaises(TypeError):
                    zim.add_item("test")
                # add an item
                testitem = item.Item(
                    namespace="C",
                    url="test.txt",
                    title="Test",
                    mimetype="text/plain",
                    is_article=True,
                    blob_source=blob.InMemoryBlobSource("content"),
                )
                zim.add_item(testitem)
                zim.flush()
                # check the entry
                entry = zim.get_entry_by_url("C", "test.txt")
                self.assertEqual(entry.namespace, "C")
                self.assertEqual(entry.url, "test.txt")
                self.assertEqual(entry.title, "Test")
                self.assertEqual(entry.mimetype, "text/plain")
                self.assertTrue(entry.is_article)
                self.assertEqual(entry.read(), b"content")

    def test_populate(self):
        """
        Populate a new ZIM file, testing that the whole writing works.
        """
        with self.open_temp_dir() as zimdir:
            with zimdir.open(mode="w", policy=self.policy) as zim:
                self.populate_zim(zim)
                zim._url_pointer_list.check_sorted()
                zim._entry_title_pointer_list.check_sorted()
                zim._article_title_pointer_list.check_sorted()
            # validate zimfile
            if self.has_zimcheck():
                self.run_zimcheck(zimdir.get_full_path())

    def test_populate_extra_flush(self):
        """
        Like L{ZimWriterTests.test_populate}, but with an extra call to .flush().

        It appears that this double flushing can be quite strenous on pyzim code.
        """
        with self.open_temp_dir() as zimdir:
            with zimdir.open(mode="w", policy=self.policy) as zim:
                self.populate_zim(zim)
                zim.flush()
                zim._url_pointer_list.check_sorted()
                zim._entry_title_pointer_list.check_sorted()
                zim._article_title_pointer_list.check_sorted()
            # validate zimfile
            if self.has_zimcheck():
                self.run_zimcheck(zimdir.get_full_path())

    def test_populate_double(self):
        """
        Populate a new ZIM file twice, testing that the whole writing works.
        """
        with self.open_temp_dir() as zimdir:
            with zimdir.open(mode="w", policy=self.policy) as zim:
                self.populate_zim(zim)
                self.populate_zim(zim)  # yes, twice
                zim._url_pointer_list.check_sorted()
                zim._entry_title_pointer_list.check_sorted()
                zim._article_title_pointer_list.check_sorted()
            # validate zimfile
            if self.has_zimcheck():
                self.run_zimcheck(zimdir.get_full_path())

    def test_populate_edit(self):
        """
        Create a new ZIM file, close it, open again and test that the whole writing works.
        """
        with self.open_temp_dir() as zimdir:
            with zimdir.open(mode="w", policy=self.policy) as zim:
                pass
            with zimdir.open(mode="u", policy=self.policy) as zim:
                self.populate_zim(zim)
                zim._url_pointer_list.check_sorted()
                zim._entry_title_pointer_list.check_sorted()
                zim._article_title_pointer_list.check_sorted()
            # validate zimfile
            if self.has_zimcheck():
                self.run_zimcheck(zimdir.get_full_path())

    def test_populate_double_edit(self):
        """
        Create a new ZIM file populate it, close it, open and populate it again and test that the whole writing works.

        You'd be surprised how many bugs this particular test case detected.
        """
        with self.open_temp_dir() as zimdir:
            with zimdir.open(mode="w", policy=self.policy) as zim:
                self.populate_zim(zim)
            with zimdir.open(mode="u", policy=self.policy) as zim:
                self.populate_zim(zim)
                zim._url_pointer_list.check_sorted()
                zim._entry_title_pointer_list.check_sorted()
                zim._article_title_pointer_list.check_sorted()
            # validate zimfile
            if self.has_zimcheck():
                self.run_zimcheck(zimdir.get_full_path())

    def test_populate_mode_x(self):
        """
        Populate a new ZIM file with mode x, testing that the whole writing works.
        """
        with self.open_temp_dir() as zimdir:
            with zimdir.open(mode="x", policy=self.policy) as zim:
                self.populate_zim(zim)
                zim._url_pointer_list.check_sorted()
                zim._entry_title_pointer_list.check_sorted()
                zim._article_title_pointer_list.check_sorted()
            # validate zimfile
            if self.has_zimcheck():
                self.run_zimcheck(zimdir.get_full_path())
            with self.assertRaises(FileExistsError):
                with zimdir.open(mode="x", policy=self.policy) as zim:
                    pass
            if self.has_zimcheck():
                self.run_zimcheck(zimdir.get_full_path())

    def test_X_uncompressed(self):
        """
        Populate a new ZIM file, testing that all entries in X namespace are uncompressed.
        """
        with self.open_temp_dir() as zimdir:
            with zimdir.open(mode="w", policy=self.policy) as zim:
                self.populate_zim(zim)
            with zimdir.open(mode="r", policy=self.policy) as zim:
                for entry in zim.iter_entries():
                    if entry.namespace == "X":
                        cluster = entry.get_cluster()
                        self.assertEqual(cluster.compression, CompressionType.NONE)
            # validate zimfile
            if self.has_zimcheck():
                self.run_zimcheck(zimdir.get_full_path())

    def test_entry_autoflush(self):
        """
        Test that autoflush works for entries.
        """
        testpolicy = policy.Policy(
            entry_cache_class=cache.LastAccessCache,
            entry_cache_kwargs={"max_size": 2},
            cluster_cache_class=cache.LastAccessCache,
            cluster_cache_kwargs={"max_size": 2},
            autoflush=True,
        )
        with self.open_temp_dir() as zimdir:
            with zimdir.open(mode="w", policy=testpolicy) as zim:
                self.populate_zim(zim)
                zim._url_pointer_list.check_sorted()
                zim._entry_title_pointer_list.check_sorted()
                zim._article_title_pointer_list.check_sorted()
                # flush zim(), then edit three articles
                zim.flush()
                zim.get_entry_by_full_url("Chome.txt").title = "Test Title"
                zim.get_entry_by_full_url("Chidden.txt")
                zim.get_entry_by_full_url("Cmarkdown.md")
                zim.entry_cache.clear()
                # Chome.txt should have been kicked out of the cache
                # and thus flushed
                home_entry = zim.get_entry_by_full_url("Chome.txt")
                self.assertFalse(home_entry.dirty)
                self.assertEqual(home_entry.title, "Test Title")
            # validate zimfile
            if self.has_zimcheck():
                self.run_zimcheck(zimdir.get_full_path())

    def test_cluster_autoflush(self):
        """
        Test that autoflush works for clusters.
        """
        testpolicy = policy.Policy(
            entry_cache_class=cache.LastAccessCache,
            entry_cache_kwargs={"max_size": 2},
            cluster_cache_class=cache.LastAccessCache,
            cluster_cache_kwargs={"max_size": 2},
            autoflush=True,
        )
        with self.open_temp_dir() as zimdir:
            with zimdir.open(mode="w", policy=testpolicy) as zim:
                self.populate_zim(zim)
                logger.info("Creating test cluster")
                cluster = zim.new_cluster()
                logger.info("Writing test cluster...")
                cluster_num = zim.write_cluster(cluster)
                cluster.append_blob(blob.InMemoryBlobSource(b"Test"))
                self.assertTrue(cluster.dirty)
                self.assertEqual(cluster.get_number_of_blobs(), 1)
                self.assertFalse(cluster.is_extended)
                self.assertEqual(cluster.get_disk_size(), 13)  # infobyte + 2 offsets (4 bytes) + bytes
                logger.info("Clearing cache...")
                zim.cluster_cache.clear()
                # after cluster cache clear, the cluster should have been flushed
                self.assertFalse(cluster.dirty)
                logger.info("Afte clearing, cluster {} is located at {}".format(cluster_num, zim._cluster_pointer_list.get_by_index(cluster_num)))
            with zimdir.open(mode="r", policy=testpolicy) as zim:
                logger.info("Getting cluster {}".format(cluster_num))
                cluster = zim.get_cluster_by_index(cluster_num)
                self.assertEqual(cluster.get_number_of_blobs(), 1)
                self.assertEqual(cluster.read_blob(0), b"Test")
            # validate zimfile
            if self.has_zimcheck():
                self.run_zimcheck(zimdir.get_full_path())

    def test_metadata_simple(self):
        """
        Test that metadata writing.
        """
        with self.open_temp_dir() as zimdir:
            with zimdir.open(mode="w", policy=self.policy) as zim:
                zim.set_metadata("Title", "testtitle")
            with zimdir.open(mode="r", policy=self.policy) as zim:
                self.assertEqual(zim.get_metadata("Title"), "testtitle")

    def test_metadata_live(self):
        """
        Test metadata writing and reading on the same object.
        """
        with self.open_temp_dir() as zimdir:
            with zimdir.open(mode="w", policy=self.policy) as zim:
                self.populate_zim(zim)
                for k, v in self.TEST_ZIM_META.items():
                    self.assertEqual(zim.get_metadata(k), v)
                # also test type errors here
                with self.assertRaises(TypeError):
                    zim.set_metadata(12, "testvalue")
                with self.assertRaises(TypeError):
                    zim.set_metadata("testkey", 12)
                with self.assertRaises(TypeError):
                    zim.set_metadata("testkey", "testvalue", mimetype=12)
                with self.assertRaises(ValueError):
                    zim.set_metadata("", "testvalue")

    def test_metadata_write(self):
        """
        Test metadata writing and reading on a written and loaded zim file.
        """
        with self.open_temp_dir() as zimdir:
            with zimdir.open(mode="w", policy=self.policy) as zim:
                self.populate_zim(zim)
            with zimdir.open(mode="r", policy=self.policy) as zim:
                for k, v in self.TEST_ZIM_META.items():
                    self.assertEqual(zim.get_metadata(k), v)
                # also test type errors here
                with self.assertRaises(TypeError):
                    zim.set_metadata(12, "testvalue")
                with self.assertRaises(TypeError):
                    zim.set_metadata("testkey", 12)
                with self.assertRaises(TypeError):
                    zim.set_metadata("testkey", "testvalue", mimetype=12)
                with self.assertRaises(ValueError):
                    zim.set_metadata("", "testvalue")

    def test_simple_live(self):
        """
        Populate a ZIM file, then read back the content without closing it.
        """
        with self.open_temp_dir() as zimdir:
            with zimdir.open(mode="w", policy=self.policy) as zim:
                # prepare ZIM
                self.populate_zim(zim)
                zim.flush()
                # home.txt
                home_entry = zim.get_entry_by_url("C", "home.txt")
                self.assertEqual(home_entry.namespace, "C")
                self.assertEqual(home_entry.url, "home.txt")
                self.assertEqual(home_entry.title, "Welcome!")
                self.assertEqual(home_entry.mimetype, "text/plain")
                self.assertEqual(home_entry.read().decode(constants.ENCODING), "This is the mainpage.")
                # /sub/directory.txt
                subdirectory_entry = zim.get_entry_by_url("C", "/sub/directory.txt")
                self.assertEqual(subdirectory_entry.namespace, "C")
                self.assertEqual(subdirectory_entry.url, "/sub/directory.txt")
                self.assertEqual(subdirectory_entry.title, "Subdirectory")
                self.assertEqual(subdirectory_entry.mimetype, "text/plain")
                self.assertEqual(subdirectory_entry.read().decode(constants.ENCODING), "subdirectory_content")
                # markdown.md
                markdown_entry = zim.get_entry_by_url("C", "markdown.md")
                self.assertEqual(markdown_entry.namespace, "C")
                self.assertEqual(markdown_entry.url, "markdown.md")
                self.assertEqual(markdown_entry.title, "Markdown")
                self.assertEqual(markdown_entry.mimetype, "text/markdown")
                self.assertEqual(markdown_entry.read().decode(constants.ENCODING), "#Markdown Test")
                # namespace.txt
                namespace_entry = zim.get_entry_by_url("T", "namespace.txt")
                self.assertEqual(namespace_entry.namespace, "T")
                self.assertEqual(namespace_entry.url, "namespace.txt")
                self.assertEqual(namespace_entry.title, "Namespace")
                self.assertEqual(namespace_entry.mimetype, "text/plain")
                self.assertEqual(namespace_entry.read().decode(constants.ENCODING), "Namespace test")
                # hidden.txt
                hidden_entry = zim.get_entry_by_url("C", "hidden.txt")
                self.assertEqual(hidden_entry.namespace, "C")
                self.assertEqual(hidden_entry.url, "hidden.txt")
                self.assertEqual(hidden_entry.title, "Hidden")
                self.assertEqual(hidden_entry.mimetype, "text/plain")
                self.assertEqual(hidden_entry.read().decode(constants.ENCODING), "hidden content")
                # mainpage test
                mainpage_entry = zim.get_mainpage_entry().resolve()
                self.assertEqual(mainpage_entry.url, home_entry.url)
                self.assertEqual(mainpage_entry.mimetype, home_entry.mimetype)
                self.assertEqual(mainpage_entry.read(), home_entry.read())

    def test_simple_write(self):
        """
        Populate a ZIM file, then close and re-open the ZIM and verify it.
        """
        with self.open_temp_dir() as zimdir:
            with zimdir.open(mode="w", policy=self.policy) as zim:
                # prepare ZIM
                self.populate_zim(zim)
            with zimdir.open(mode="r", policy=self.policy) as zim:
                # home.txt
                home_entry = zim.get_entry_by_url("C", "home.txt")
                self.assertEqual(home_entry.namespace, "C")
                self.assertEqual(home_entry.url, "home.txt")
                self.assertEqual(home_entry.title, "Welcome!")
                self.assertEqual(home_entry.mimetype, "text/plain")
                self.assertEqual(home_entry.read().decode(constants.ENCODING), "This is the mainpage.")
                self.assertTrue(home_entry.is_article)
                # /sub/directory.txt
                subdirectory_entry = zim.get_entry_by_url("C", "/sub/directory.txt")
                self.assertEqual(subdirectory_entry.namespace, "C")
                self.assertEqual(subdirectory_entry.url, "/sub/directory.txt")
                self.assertEqual(subdirectory_entry.title, "Subdirectory")
                self.assertEqual(subdirectory_entry.mimetype, "text/plain")
                self.assertEqual(subdirectory_entry.read().decode(constants.ENCODING), "subdirectory_content")
                self.assertTrue(subdirectory_entry.is_article)
                # markdown.md
                markdown_entry = zim.get_entry_by_url("C", "markdown.md")
                self.assertEqual(markdown_entry.namespace, "C")
                self.assertEqual(markdown_entry.url, "markdown.md")
                self.assertEqual(markdown_entry.title, "Markdown")
                self.assertEqual(markdown_entry.mimetype, "text/markdown")
                self.assertEqual(markdown_entry.read().decode(constants.ENCODING), "#Markdown Test")
                self.assertTrue(markdown_entry.is_article)
                # namespace.txt
                namespace_entry = zim.get_entry_by_url("T", "namespace.txt")
                self.assertEqual(namespace_entry.namespace, "T")
                self.assertEqual(namespace_entry.url, "namespace.txt")
                self.assertEqual(namespace_entry.title, "Namespace")
                self.assertEqual(namespace_entry.mimetype, "text/plain")
                self.assertEqual(namespace_entry.read().decode(constants.ENCODING), "Namespace test")
                self.assertFalse(namespace_entry.is_article)
                # hidden.txt
                hidden_entry = zim.get_entry_by_url("C", "hidden.txt")
                self.assertEqual(hidden_entry.namespace, "C")
                self.assertEqual(hidden_entry.url, "hidden.txt")
                self.assertEqual(hidden_entry.title, "Hidden")
                self.assertEqual(hidden_entry.mimetype, "text/plain")
                self.assertEqual(hidden_entry.read().decode(constants.ENCODING), "hidden content")
                # mainpage test
                mainpage_entry = zim.get_mainpage_entry().resolve()
                self.assertEqual(mainpage_entry.url, home_entry.url)
                self.assertEqual(mainpage_entry.mimetype, home_entry.mimetype)
                self.assertEqual(mainpage_entry.read(), home_entry.read())

    def test_articles_live(self):
        """
        Populate a ZIM file and check that the article title pointer is correct without closing it.
        """
        with self.open_temp_dir() as zimdir:
            with zimdir.open(mode="w", policy=self.policy) as zim:
                self.populate_zim(zim)
                zim.flush()
                article_titles = list(zim._article_title_pointer_list.iter_values())
                self.assertEqual(len(article_titles), 3)
                self.assertIn(b"Welcome!", article_titles)
                self.assertIn(b"Subdirectory", article_titles)
                self.assertIn(b"Markdown", article_titles)
                n_iter = 0
                for article in zim.iter_articles():
                    self.assertIn(article.full_url, ["Chome.txt", "C/sub/directory.txt", "Cmarkdown.md"])
                    n_iter += 1
                self.assertEqual(n_iter, 3)

    def test_articles_write(self):
        """
        Populate a ZIM file and check that the article title pointer is correct with a re-opened file.
        """
        with self.open_temp_dir() as zimdir:
            with zimdir.open(mode="w", policy=self.policy) as zim:
                self.populate_zim(zim)
            with zimdir.open(mode="r", policy=self.policy) as zim:
                article_titles = list(zim._article_title_pointer_list.iter_values())
                self.assertEqual(len(article_titles), 3)
                self.assertIn(b"Welcome!", article_titles)
                self.assertIn(b"Subdirectory", article_titles)
                self.assertIn(b"Markdown", article_titles)
                n_iter = 0
                for article in zim.iter_articles():
                    self.assertIn(article.full_url, ["Chome.txt", "C/sub/directory.txt", "Cmarkdown.md"])
                    n_iter += 1
                self.assertEqual(n_iter, 3)

    def test_articles_edit_live(self):
        """
        Populate a ZIM file and edit it to change article status.
        """
        with self.open_temp_dir() as zimdir:
            with zimdir.open(mode="w", policy=self.policy) as zim:
                self.populate_zim(zim)
                zim.flush()
                home_entry = zim.get_entry_by_full_url("Chome.txt")
                home_entry.is_article = False
                home_entry.flush()
                hidden_entry = zim.get_entry_by_full_url("Chidden.txt")
                hidden_entry.is_article = True
                hidden_entry.flush()

                article_titles = list(zim._article_title_pointer_list.iter_values())
                self.assertEqual(len(article_titles), 3)
                self.assertNotIn(b"Welcome!", article_titles)
                self.assertIn(b"Hidden", article_titles)
                self.assertIn(b"Subdirectory", article_titles)
                self.assertIn(b"Markdown", article_titles)
                n_iter = 0
                for article in zim.iter_articles():
                    self.assertIn(article.full_url, ["C/sub/directory.txt", "Cmarkdown.md", "Chidden.txt"])
                    n_iter += 1
                self.assertEqual(n_iter, 3)

    def test_articles_edit_write(self):
        """
        Populate a ZIM file and edit it to change article status.
        """
        with self.open_temp_dir() as zimdir:
            with zimdir.open(mode="w", policy=self.policy) as zim:
                self.populate_zim(zim)
                zim.flush()
                home_entry = zim.get_entry_by_full_url("Chome.txt")
                home_entry.is_article = False
                home_entry.flush()
                hidden_entry = zim.get_entry_by_full_url("Chidden.txt")
                hidden_entry.is_article = True
                hidden_entry.flush()
            with zimdir.open(mode="r") as zim:
                article_titles = list(zim._article_title_pointer_list.iter_values())
                self.assertEqual(len(article_titles), 3)
                self.assertNotIn(b"Welcome!", article_titles)
                self.assertIn(b"Hidden", article_titles)
                self.assertIn(b"Subdirectory", article_titles)
                self.assertIn(b"Markdown", article_titles)
                n_iter = 0
                for article in zim.iter_articles():
                    self.assertIn(article.full_url, ["C/sub/directory.txt", "Cmarkdown.md", "Chidden.txt"])
                    n_iter += 1
                self.assertEqual(n_iter, 3)

    def test_title_edit_live(self):
        """
        Populate a ZIM file and edit it to change titles.
        """
        with self.open_temp_dir() as zimdir:
            with zimdir.open(mode="w", policy=self.policy) as zim:
                self.populate_zim(zim)
                zim.flush()
                home_entry = zim.get_entry_by_full_url("Chome.txt")
                home_entry.title = "Test Title"
                home_entry.flush()

                article_titles = list(zim._article_title_pointer_list.iter_values())
                self.assertEqual(len(article_titles), 3)
                self.assertNotIn(b"Welcome!", article_titles)
                self.assertIn(b"Test Title", article_titles)
                self.assertIn(b"Subdirectory", article_titles)
                self.assertIn(b"Markdown", article_titles)
                entry_titles = list(zim._entry_title_pointer_list.iter_values())
                self.assertEqual(len(entry_titles), self.NUM_ENTRIES)
                self.assertNotIn(b"CWelcome!", entry_titles)
                self.assertIn(b"CTest Title", entry_titles)
                self.assertIn(b"CSubdirectory", entry_titles)
                self.assertIn(b"CMarkdown", entry_titles)
                self.assertIn(b"TNamespace", entry_titles)
                self.assertIn(b"CHidden", entry_titles)

    def test_title_edit_write(self):
        """
        Populate a ZIM file and edit it to change titles, then write it and read it again.
        """
        with self.open_temp_dir() as zimdir:
            with zimdir.open(mode="w", policy=self.policy) as zim:
                self.populate_zim(zim)
                zim.flush()
                home_entry = zim.get_entry_by_full_url("Chome.txt")
                home_entry.title = "Test Title"
                home_entry.flush()
            with zimdir.open(mode="r", policy=self.policy) as zim:
                article_titles = list(zim._article_title_pointer_list.iter_values())
                self.assertEqual(len(article_titles), 3)
                self.assertNotIn(b"Welcome!", article_titles)
                self.assertIn(b"Test Title", article_titles)
                self.assertIn(b"Subdirectory", article_titles)
                self.assertIn(b"Markdown", article_titles)
                entry_titles = list(zim._entry_title_pointer_list.iter_values())
                self.assertEqual(len(entry_titles), self.NUM_ENTRIES)
                self.assertNotIn(b"CWelcome!", entry_titles)
                self.assertIn(b"CTest Title", entry_titles)
                self.assertIn(b"CSubdirectory", entry_titles)
                self.assertIn(b"CMarkdown", entry_titles)
                self.assertIn(b"TNamespace", entry_titles)
                self.assertIn(b"CHidden", entry_titles)

    def test_truncate(self):
        """
        Test archive self-truncation.
        """
        testpolicy = policy.Policy(truncate=True)
        with self.open_temp_dir() as zimdir:
            with zimdir.open(mode="w", policy=testpolicy) as zim:
                self.populate_zim(zim)
                zim.flush()
                with zim.acquire_file() as f:
                    old_end = zim.header.checksum_position + 16
                    f.seek(old_end)
                    f.write(b"test" * 512)
                zim.mark_dirty()  # otherwise, flush() won't do anything
                zim.flush()
                # now, the last couple of bytes should have been truncated
                with zim.acquire_file() as f:
                    f.seek(0, 2)  # seek to file end
                    self.assertEqual(f.tell(), zim.header.checksum_position + 16)
                    self.assertEqual(f.tell(), old_end)
        # repeat without truncate
        testpolicy = policy.Policy(truncate=False)
        with self.open_temp_dir() as zimdir:
            with zimdir.open(mode="w", policy=testpolicy) as zim:
                self.populate_zim(zim)
                zim.flush()
                with zim.acquire_file() as f:
                    old_end = zim.header.checksum_position + 16
                    f.seek(old_end)
                    f.write(b"test" * 512)
                zim.mark_dirty()
                zim.flush()
                # now, the last couple of bytes should have been truncated
                with zim.acquire_file() as f:
                    f.seek(0, 2)  # seek to file end
                    self.assertNotEqual(f.tell(), zim.header.checksum_position + 16)
                    self.assertEqual(f.tell(), old_end + (4 * 512))

    def test_set_mainpage_url(self):
        """
        Test L{pyzim.archive.Zim.set_mainpage_url}.
        """
        with self.open_temp_dir() as zimdir:
            with zimdir.open(mode="w", policy=self.policy) as zim:
                # add an entry that we can link to
                self.add_item(zim, "C", "mainpage.html", "Main Page", "text/html", "content")
                self.add_item(zim, "C", "alternative.html", "Alternative Page", "text/html", "alternative")
                # a new archive should not contain a mainapge entry
                with self.assertRaises(exceptions.EntryNotFound):
                    zim.get_mainpage_entry()
                # setting an invalid value should raise an exception, but
                # not modify the content
                with self.assertRaises(TypeError):
                    zim.set_mainpage_url(12)
                zim.flush()
                with self.assertRaises(exceptions.EntryNotFound):
                    zim.get_mainpage_entry()
                # set the URL
                zim.set_mainpage_url("mainpage.html")
                zim.flush()
                # validate entry
                entry = zim.get_mainpage_entry().resolve()
                self.assertEqual(entry.title, "Main Page")
                self.assertEqual(entry.read(), b"content")
                # ensure that setting an invalid value does not change
                # the mainpage redirect
                with self.assertRaises(TypeError):
                    zim.set_mainpage_url(12)
                zim.flush()
                entry = zim.get_mainpage_entry().resolve()
                self.assertEqual(entry.title, "Main Page")
                self.assertEqual(entry.read(), b"content")
                # change the value
                zim.set_mainpage_url("alternative.html")
                zim.flush()
                entry = zim.get_mainpage_entry().resolve()
                self.assertEqual(entry.title, "Alternative Page")
                self.assertEqual(entry.read(), b"alternative")
                # delete entry
                zim.set_mainpage_url(None)
                zim.flush()
                with self.assertRaises(exceptions.EntryNotFound):
                    zim.get_mainpage_entry()
            with self.assertRaises(exceptions.UnresolvedRedirect):
                with zimdir.open(mode="w") as zim:
                    # test for unresolvable url
                    zim.set_mainpage_url("nonexistent.html")
                    zim.flush()

    def test_remove_entry_by_full_url(self):
        """
        Test L{pyzim.archive.Zim.remove_entry_by_full_url}.
        """
        with self.open_temp_dir() as zimdir:
            with zimdir.open(mode="w", policy=self.policy) as zim:
                # check invalid values
                with self.assertRaises(TypeError):
                    zim.remove_entry_by_full_url(12)
                with self.assertRaises(TypeError):
                    zim.remove_entry_by_full_url("Chome.txt", 12)
                with self.assertRaises(ValueError):
                    zim.remove_entry_by_full_url("Chome.txt", blob="test")
                # populate the ZIM
                self.populate_zim(zim)
                zim.flush()
                # get some titles for later
                entry_titles = [e.title for e in zim.iter_entries()]
                article_titles = [e.title for e in zim.iter_articles()]
                # the mainpage entry should be a redirect
                # consequently, the blob should not be removed
                self.assertEqual(zim.get_mainpage_entry().resolve().read(), b"This is the mainpage.")
                zim.remove_entry_by_full_url(zim.get_mainpage_entry().full_url, blob="remove")
                # entry should now be removed
                with self.assertRaises(exceptions.EntryNotFound):
                    zim.get_mainpage_entry()
                # but linked entry and blob should still be there
                self.assertEqual(zim.get_entry_by_full_url("Chome.txt").read(), b"This is the mainpage.")
                # let's remove the main entry directly, but keep the blob
                entry = zim.get_entry_by_full_url("Chome.txt")
                zim.remove_entry_by_full_url(entry.full_url, blob="keep")
                with self.assertRaises(exceptions.EntryNotFound):
                    zim.get_entry_by_full_url(entry.full_url)
                # check that the blob is still there
                self.assertEqual(zim.get_cluster_by_index(entry.cluster_number).read_blob(entry.blob_number), b"This is the mainpage.")
                # ensure the title lists have been correctly updated.
                new_entry_titles = [e.title for e in zim.iter_entries()]
                new_article_titles = [e.title for e in zim.iter_articles()]
                while "Welcome!" in entry_titles:
                    entry_titles.remove("Welcome!")
                article_titles.remove("Welcome!")
                self.assertEqual(new_entry_titles, entry_titles)
                self.assertEqual(new_article_titles, article_titles)
            # open the ZIM again, so we can re-populate it and use the same testing strategy
            with zimdir.open(mode="w", policy=self.policy) as zim:
                self.populate_zim(zim)
                zim.flush()
                # remove an entry and empty the blob
                entry = zim.get_entry_by_full_url("Chome.txt")
                zim.remove_entry_by_full_url(entry.full_url, blob="empty")
                with self.assertRaises(exceptions.EntryNotFound):
                    zim.get_entry_by_full_url(entry.full_url)
                # check that the blob is empty
                self.assertEqual(zim.get_cluster_by_index(entry.cluster_number).read_blob(entry.blob_number), b"")
            # open the ZIM again, so we can re-populate it and use the same testing strategy
            with zimdir.open(mode="w", policy=self.policy) as zim:
                self.populate_zim(zim)
                zim.flush()
                # remove an entry and remove the blob
                entry = zim.get_entry_by_full_url("Chome.txt")
                old_num_blobs = zim.get_cluster_by_index(entry.cluster_number).get_number_of_blobs()
                zim.remove_entry_by_full_url(entry.full_url, blob="remove")
                with self.assertRaises(exceptions.EntryNotFound):
                    zim.get_entry_by_full_url(entry.full_url)
                # check that the blob has been removed
                self.assertEqual(zim.get_cluster_by_index(entry.cluster_number).get_number_of_blobs(), old_num_blobs - 1)
                self.assertNotIn(zim.get_cluster_by_index(entry.cluster_number).read_blob(entry.blob_number), (b"", b"This is the mainpage."))
            # now, test that redirects will still work even if we remove an entry
            with zimdir.open(mode="w", policy=self.policy) as zim:
                self.populate_zim(zim)
                zim.flush()
                zim.remove_entry_by_full_url("Cmarkdown.md")
                zim.flush()
                # check that the redirect still works
                self.assertEqual(zim.get_mainpage_entry().resolve().title, "Welcome!")

    def test_entry_at_url_is_article_live(self):
        """
        Test L{pyzim.archive.Zim.entry_at_url_is_article} on the same archive.
        """
        with self.open_temp_dir() as zimdir:
            with zimdir.open(mode="w", policy=self.policy) as zim:
                # check invalid values
                with self.assertRaises(TypeError):
                    zim.entry_at_url_is_article(12)
                with self.assertRaises(ValueError):
                    zim.entry_at_url_is_article("Tnamespace.txt")
                # populate ZIM
                self.populate_zim(zim)
                zim.flush()
                # test ZIM articles
                self.assertTrue(zim.entry_at_url_is_article("Chome.txt"))
                self.assertFalse(zim.entry_at_url_is_article("Chidden.txt"))
                self.assertFalse(zim.entry_at_url_is_article("Cnonexistent.txt"))

    def test_entry_at_url_is_article_write(self):
        """
        Test L{pyzim.archive.Zim.entry_at_url_is_article} while reloading the archive.
        """
        with self.open_temp_dir() as zimdir:
            with zimdir.open(mode="w", policy=self.policy) as zim:
                # check invalid values
                with self.assertRaises(TypeError):
                    zim.entry_at_url_is_article(12)
                with self.assertRaises(ValueError):
                    zim.entry_at_url_is_article("Tnamespace.txt")
                # populate ZIM
                self.populate_zim(zim)
            with zimdir.open(mode="r", policy=self.policy) as zim:
                # test ZIM articles
                self.assertTrue(zim.entry_at_url_is_article("Chome.txt"))
                self.assertFalse(zim.entry_at_url_is_article("Chidden.txt"))
                self.assertFalse(zim.entry_at_url_is_article("Cnonexistent.txt"))

    def test_write_cluster(self):
        """
        Test L{pyzim.archive.Zim.write_cluster}.
        """
        with self.open_temp_dir() as zimdir:
            with zimdir.open(mode="w", policy=self.policy) as zim:
                # test invalid values
                with self.assertRaises(TypeError):
                    zim.write_cluster("test")
                with self.assertRaises(TypeError):
                    zim.write_cluster(cluster.EmptyCluster())
                new_cluster = zim.new_cluster()
                with self.assertRaises(TypeError):
                    zim.write_cluster(new_cluster, cluster_num="test")
                with self.assertRaises(ValueError):
                    zim.write_cluster(new_cluster, cluster_num=-1)
                # test with non-bound cluster
                new_cluster.unbind()
                with self.assertRaises(exceptions.BindingError):
                    zim.write_cluster(new_cluster)
                # create and write a cluster
                new_cluster = zim.new_cluster()
                new_cluster.append_blob(blob.InMemoryBlobSource("content"))
                cluster_num = zim.write_cluster(new_cluster)
                zim.cluster_cache.clear()
                cluster_loaded = zim.get_cluster_by_index(cluster_num)
                self.assertEqual(cluster_loaded.read_blob(0), b"content")
                # overwrite cluster
                cluster_loaded.set_blob(0, blob.InMemoryBlobSource("test"))
                second_cluster_num = zim.write_cluster(cluster_loaded)
                self.assertEqual(cluster_num, second_cluster_num)
                zim.cluster_cache.clear()
                cluster_loaded = zim.get_cluster_by_index(cluster_num)
                self.assertEqual(cluster_loaded.read_blob(0), b"test")
                # test with specified blob_num
                cluster_loaded.set_blob(0, blob.InMemoryBlobSource("foo"))
                second_cluster_num = zim.write_cluster(cluster_loaded, cluster_num)
                self.assertEqual(cluster_num, second_cluster_num)
                zim.cluster_cache.clear()
                cluster_loaded = zim.get_cluster_by_index(cluster_num)
                self.assertEqual(cluster_loaded.read_blob(0), b"foo")

    def test_add_full_url_redirect(self):
        """
        Test L{pyzim.archive.Zim.add_full_url_redirect}.
        """
        with self.open_temp_dir() as zimdir:
            with zimdir.open(mode="w", policy=self.policy) as zim:
                # test invalid values
                with self.assertRaises(TypeError):
                    zim.add_full_url_redirect(1, "target")
                with self.assertRaises(TypeError):
                    zim.add_full_url_redirect("source", 2)
                with self.assertRaises(TypeError):
                    zim.add_full_url_redirect("source", "target", 3)
                with self.assertRaises(ValueError):
                    zim.add_full_url_redirect("source", "source")
                with self.assertRaises(ValueError):
                    zim.add_full_url_redirect("", "target")
                with self.assertRaises(ValueError):
                    zim.add_full_url_redirect("source", "")
                # populate ZIM
                self.populate_zim(zim)
                # add unresolvable redirect
                zim.add_full_url_redirect("Credirect", "Centry.txt")
                with self.assertRaises(exceptions.UnresolvedRedirect):
                    zim.flush()
                # the redirect should still be buffered, so adding a target
                # should make it work
                self.add_item(zim, "C", "entry.txt", "Entry Title", "text/plain", "content")
                zim.flush()
                self.assertEqual(zim.get_entry_by_full_url("Credirect").resolve().read(), b"content")
                # add a redirect directly
                # this time, no flush should be needed
                zim.add_full_url_redirect("Credirect2", "Centry.txt")
                self.assertEqual(zim.get_entry_by_full_url("Credirect2").resolve().read(), b"content")
                # adding a redirect with the same URL should overwrite the previous redirect
                zim.add_full_url_redirect("Credirect", "Cmarkdown.md")
                self.assertEqual(zim.get_entry_by_full_url("Credirect").resolve().read(), b"#Markdown Test")
            # use a new ZIM
            with zimdir.open(mode="w", policy=self.policy) as zim:
                # this time, add multiple, recursive, initially unresolvable redirects
                # also use multiple namespaces and titles here
                zim.add_full_url_redirect("Credirect1", "Centry.txt")  # no specific title
                zim.add_full_url_redirect("Tredirect2", "Credirect1", title="Redirect 2")
                zim.add_full_url_redirect("Credirect3", "Tredirect2")  # no specific title
                # make it resolvable
                self.add_item(zim, "C", "entry.txt", "Entry Title", "text/plain", "content")
                # flush
                zim.flush()
                # redirects should now be resolvable
                self.assertEqual(zim.get_entry_by_full_url("Credirect1").resolve().read(), b"content")
                self.assertEqual(zim.get_entry_by_full_url("Tredirect2").resolve().read(), b"content")
                self.assertEqual(zim.get_entry_by_full_url("Credirect3").resolve().read(), b"content")
                # check title behavior
                self.assertEqual(zim.get_entry_by_full_url("Credirect1").title, "Entry Title")  # from entry
                self.assertEqual(zim.get_entry_by_full_url("Tredirect2").title, "Redirect 2")  # specifically set
                self.assertEqual(zim.get_entry_by_full_url("Credirect3").title, "Redirect 2")  # from redirect 2

    def test_add_redirect(self):
        """
        Test L{pyzim.archive.Zim.add_redirect}.
        """
        with self.open_temp_dir() as zimdir:
            with zimdir.open(mode="w", policy=self.policy) as zim:
                # test invalid values
                with self.assertRaises(TypeError):
                    zim.add_redirect(1, "target")
                with self.assertRaises(TypeError):
                    zim.add_redirect("source", 2)
                with self.assertRaises(TypeError):
                    zim.add_redirect("source", "target", 3)
                with self.assertRaises(ValueError):
                    zim.add_redirect("source", "source")
                # populate ZIM
                self.populate_zim(zim)
                # add unresolvable redirect
                zim.add_redirect("redirect", "entry.txt")
                with self.assertRaises(exceptions.UnresolvedRedirect):
                    zim.flush()
                # the redirect should still be buffered, so adding a target
                # should make it work
                self.add_item(zim, "C", "entry.txt", "Entry Title", "text/plain", "content")
                zim.flush()
                self.assertEqual(zim.get_entry_by_full_url("Credirect").resolve().read(), b"content")
                # add a redirect directly
                # this time, no flush should be needed
                zim.add_redirect("redirect2", "entry.txt")
                self.assertEqual(zim.get_entry_by_full_url("Credirect2").resolve().read(), b"content")
                # adding a redirect with the same URL should overwrite the previous redirect
                zim.add_redirect("redirect", "markdown.md")
                self.assertEqual(zim.get_entry_by_full_url("Credirect").resolve().read(), b"#Markdown Test")
            # use a new ZIM
            with zimdir.open(mode="w", policy=self.policy) as zim:
                self.populate_zim(zim)
                # this time, add multiple, recursive, initially unresolvable redirects
                # also use multiple namespaces and titles here
                zim.add_redirect("redirect1", "entry.txt")  # no specific title
                zim.add_redirect("redirect2", "redirect1", title="Redirect 2")
                zim.add_redirect("redirect3", "redirect2")  # no specific title
                # make it resolvable
                self.add_item(zim, "C", "entry.txt", "Entry Title", "text/plain", "content")
                # flush
                zim.flush()
                # redirects should now be resolvable
                self.assertEqual(zim.get_entry_by_full_url("Credirect1").resolve().read(), b"content")
                self.assertEqual(zim.get_entry_by_full_url("Credirect2").resolve().read(), b"content")
                self.assertEqual(zim.get_entry_by_full_url("Credirect3").resolve().read(), b"content")
                # check title behavior
                self.assertEqual(zim.get_entry_by_full_url("Credirect1").title, "Entry Title")  # from entry
                self.assertEqual(zim.get_entry_by_full_url("Credirect2").title, "Redirect 2")  # specifically set
                self.assertEqual(zim.get_entry_by_full_url("Credirect3").title, "Redirect 2")  # from redirect 2

    def test_edit_url(self):
        """
        Test editing of the URL of an existing entry.
        """
        with self.open_temp_dir() as zimdir:
            with zimdir.open(mode="w", policy=self.policy) as zim:
                # prepare ZIM
                self.populate_zim(zim)
                zim.add_redirect("redirect", "home.txt")
                zim.add_redirect("redirect_m", "markdown.md", title="Markdown redirect")
                zim.flush()
                # get an entry and change the URL
                entry = zim.get_entry_by_full_url("Chome.txt")
                entry.url = "new_url.txt"
                entry.flush()
                # check the entry
                entry = zim.get_entry_by_full_url("Cnew_url.txt")
                self.assertEqual(entry.title, "Welcome!")
                self.assertEqual(entry.full_url, "Cnew_url.txt")
                self.assertEqual(entry.mimetype, "text/plain")
                self.assertEqual(entry.read(), b"This is the mainpage.")
                # check that the redirect works
                self.assertEqual(zim.get_entry_by_full_url("Credirect").resolve().full_url, "Cnew_url.txt")
                self.assertEqual(zim.get_entry_by_full_url("Credirect_m").follow().full_url, "Cmarkdown.md")
                self.assertEqual(zim.get_entry_by_full_url("Credirect_m").title, "Markdown redirect")
                self.assertEqual(zim.get_entry_by_full_url("Credirect_m").resolve().title, "Markdown")
                self.assertEqual(zim.get_mainpage_entry().resolve().full_url, "Cnew_url.txt")
                # validate sorting of title pointer lists
                zim._entry_title_pointer_list.check_sorted()
                zim._article_title_pointer_list.check_sorted()
            if self.has_zimcheck():
                self.run_zimcheck(zimdir.get_full_path())

    def test_edit_title(self):
        """
        Test editing of the title of an existing entry.
        """
        with self.open_temp_dir() as zimdir:
            with zimdir.open(mode="w", policy=self.policy) as zim:
                # prepare ZIM
                self.populate_zim(zim)
                zim.flush()
                # get an article entry and change the title
                entry = zim.get_entry_by_full_url("Chome.txt")
                entry.title = "New Title"
                entry.flush()
                # check the entry
                entry = zim.get_entry_by_full_url("Chome.txt")
                self.assertEqual(entry.title, "New Title")
                self.assertEqual(entry.full_url, "Chome.txt")
                self.assertEqual(entry.mimetype, "text/plain")
                self.assertEqual(entry.read(), b"This is the mainpage.")
                self.assertTrue(entry.is_article)
                titles = [e.title if not e.is_redirect else None for e in zim.iter_entries()]
                article_titles = [e.title if not e.is_redirect else None for e in zim.iter_articles()]
                self.assertIn("New Title", titles)
                self.assertNotIn("Welcome!", titles)
                self.assertIn("New Title", article_titles)
                self.assertNotIn("Welcome!", article_titles)
                zim._entry_title_pointer_list.check_sorted()
                zim._article_title_pointer_list.check_sorted()
                # check with non-article entry
                entry = zim.get_entry_by_full_url("Chidden.txt")
                entry.title = "New hidden Title"
                entry.flush()
                # check the entry
                entry = zim.get_entry_by_full_url("Chidden.txt")
                self.assertEqual(entry.title, "New hidden Title")
                self.assertEqual(entry.full_url, "Chidden.txt")
                self.assertEqual(entry.mimetype, "text/plain")
                self.assertEqual(entry.read(), b"hidden content")
                self.assertFalse(entry.is_article)
                titles = [e.title if not e.is_redirect else None for e in zim.iter_entries()]
                article_titles = [e.title if not e.is_redirect else None for e in zim.iter_articles()]
                self.assertIn("New hidden Title", titles)
                self.assertNotIn("Hidden", titles)
                self.assertNotIn("New hidden Title", article_titles)
                self.assertNotIn("Hidden", article_titles)
                zim._entry_title_pointer_list.check_sorted()
                zim._article_title_pointer_list.check_sorted()
            if self.has_zimcheck():
                self.run_zimcheck(zimdir.get_full_path())

    def test_edit_is_article(self):
        """
        Test editing of the article status of an existing entry.
        """
        with self.open_temp_dir() as zimdir:
            with zimdir.open(mode="w", policy=self.policy) as zim:
                # prepare ZIM
                self.populate_zim(zim)
                zim.flush()
                # get an article entry and change the title
                entry = zim.get_entry_by_full_url("Chome.txt")
                entry.is_article = False
                entry.flush()
                # check the entry
                entry = zim.get_entry_by_full_url("Chome.txt")
                self.assertEqual(entry.title, "Welcome!")
                self.assertEqual(entry.full_url, "Chome.txt")
                self.assertEqual(entry.mimetype, "text/plain")
                self.assertEqual(entry.read(), b"This is the mainpage.")
                self.assertFalse(entry.is_article)
                titles = [e.title if not e.is_redirect else None for e in zim.iter_entries()]
                article_titles = [e.title if not e.is_redirect else None for e in zim.iter_articles()]
                self.assertIn("Welcome!", titles)
                self.assertNotIn("Welcome!", article_titles)
                zim._entry_title_pointer_list.check_sorted()
                zim._article_title_pointer_list.check_sorted()
                # check with non-article entry
                entry = zim.get_entry_by_full_url("Chidden.txt")
                entry.is_article = True
                entry.flush()
                # check the entry
                entry = zim.get_entry_by_full_url("Chidden.txt")
                self.assertEqual(entry.title, "Hidden")
                self.assertEqual(entry.full_url, "Chidden.txt")
                self.assertEqual(entry.mimetype, "text/plain")
                self.assertEqual(entry.read(), b"hidden content")
                self.assertTrue(entry.is_article)
                titles = [e.title if not e.is_redirect else None for e in zim.iter_entries()]
                article_titles = [e.title if not e.is_redirect else None for e in zim.iter_articles()]
                self.assertIn("Hidden", titles)
                self.assertIn("Hidden", article_titles)
                zim._entry_title_pointer_list.check_sorted()
                zim._article_title_pointer_list.check_sorted()
            if self.has_zimcheck():
                self.run_zimcheck(zimdir.get_full_path())


# ============== ALTERNATE POLICY TESTS ==================

class LowRamZimReaderTests(ZimReaderTests):
    """
    Like L{ZimReaderTests}, but using L{pyzim.policy.LOW_RAM_DECOMP_POLICY}.
    """
    policy = policy.LOW_RAM_DECOMP_POLICY
