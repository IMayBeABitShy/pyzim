"""
Tests for L{pyzim.search}.
"""
import unittest
from unittest import mock

from pyzim import exceptions, constants
from pyzim.search import TitleStartSearch, XapianSearch

from .base import TestBase


class SearchReadTests(unittest.TestCase, TestBase):
    """
    Tests for the pyzim search in read-mode.
    """
    def test_default_search(self):
        """
        Test L{pyzim.archive.Zim.get_search}.
        """
        with self.open_zts_zim("nons", "wikipedia_en_climate_change_mini_2024-06.zim") as zim:
            # test xapian fulltext
            search = zim.get_search()
            self.assertIsInstance(search, XapianSearch)
            self.assertEqual(search._full_url, constants.URL_XAPIAN_FULLTEXT_INDEX)
            search.close()
            # test xapian title
            # we test this by changing the index url to some nonsense
            with mock.patch("pyzim.constants.URL_XAPIAN_FULLTEXT_INDEX", "Xdoes/not/exist"):
                search = zim.get_search()
            self.assertIsInstance(search, XapianSearch)
            self.assertEqual(search._full_url, constants.URL_XAPIAN_TITLE_INDEX)
            search.close()
            # test non-xapian search
            with mock.patch("pyzim.search.xapian", None):
                search = zim.get_search()
            self.assertIsInstance(search, TitleStartSearch)

    def test_titlestart_search_simple(self):
        """
        Test a simple search using the L{pyzim.search.TitleStartSearch}.
        """
        with self.open_zts_zim("nons", "wikipedia_en_climate_change_mini_2024-06.zim") as zim:
            search = TitleStartSearch(zim)
            results = search.search("German")
            self.assertGreater(len(results), 0)
            titles = []
            for result in results:
                self.assertTrue(result.title.startswith("German"))
                entry = zim.get_entry_by_full_url(result.full_url)
                self.assertEqual(result.title, entry.title)
                titles.append(result.title)
            self.assertEqual(results.n_estimated, len(titles))
            self.assertIn("German Climate Consortium", titles)
            self.assertNotIn("Germanic bog sacrifice", titles)  # it's a non-article entry
            search.close()

    def test_titlestart_search_empty(self):
        """
        Test a search using the L{pyzim.search.TitleStartSearch} and an empty search term.
        """
        with self.open_zts_zim("nons", "wikipedia_en_climate_change_mini_2024-06.zim") as zim:
            search = TitleStartSearch(zim)
            results = search.search("")
            self.assertGreater(len(results), 0)
            self.assertLessEqual(len(results), zim.header.entry_count)
            titles = []
            for result in results:
                entry = zim.get_entry_by_full_url(result.full_url)
                self.assertEqual(result.title, entry.title)
                self.assertTrue(entry.is_article)
                titles.append(result.title)
            self.assertEqual(results.n_estimated, len(titles))
            self.assertIn("German Climate Consortium", titles)
            self.assertNotIn("Germanic bog sacrifice", titles)  # it's a non-article entry
            search.close()

    def test_titlestart_search_nonarticle(self):
        """
        Test non-article-including search using the L{pyzim.search.TitleStartSearch}.
        """
        with self.open_zts_zim("nons", "wikipedia_en_climate_change_mini_2024-06.zim") as zim:
            search = TitleStartSearch(zim, search_nonarticles=True)
            results = search.search("CGerman")  # remember to specify namespace
            self.assertGreater(len(results), 0)
            titles = []
            for result in results:
                self.assertTrue(result.title.startswith("German"))
                entry = zim.get_entry_by_full_url(result.full_url)
                self.assertEqual(result.title, entry.title)
                titles.append(result.title)
            self.assertEqual(results.n_estimated, len(titles))
            self.assertIn("German Climate Consortium", titles)
            self.assertIn("Germanic bog sacrifice", titles)
            search.close()

    def test_titlestart_search_range(self):
        """
        Test a range-bound search using the L{pyzim.search.TitleStartSearch}.
        """
        with self.open_zts_zim("nons", "wikipedia_en_climate_change_mini_2024-06.zim") as zim:
            search = TitleStartSearch(zim)
            results = search.search("Clean")
            self.assertGreater(len(results), 0)
            titles = []
            for result in results:
                self.assertTrue(result.title.startswith("Clean"))
                titles.append(result.title)
            self.assertGreaterEqual(len(titles), 10)
            self.assertEqual(results.n_estimated, len(titles))
            n = 0
            for result in results.iter_in_range(3, 4):
                n += 1
                self.assertIn(result.title, titles[2:6])
            self.assertEqual(n, 4)
            # test fetching more results than available
            n = 0
            for result in results.iter_in_range(5, 10**8):
                n += 1
            self.assertLess(n, 10**5)
            search.close()

    def test_xapian_init_fail(self):
        """
        Test L{pyzim.search.XapianSearch.__init__} raising exceptions.
        """
        with self.open_zts_zim("nons", "wikipedia_en_climate_change_mini_2024-06.zim") as zim:
            with mock.patch("pyzim.search.xapian", None):
                with self.assertRaises(exceptions.MissingDependency):
                    XapianSearch.open_title(zim).close()
            with mock.patch("pyzim.search.langs", None):
                with self.assertRaises(exceptions.MissingDependency):
                    XapianSearch.open_title(zim).close()
            with self.assertRaises(exceptions.ZimFeatureMissing):
                XapianSearch(zim, full_url="Xdoes/not/exist").close()

    def test_xapian_title_search(self):
        """
        Test a simple search using the xapian title index.
        """
        with self.open_zts_zim("nons", "wikipedia_en_climate_change_mini_2024-06.zim") as zim:
            search = XapianSearch.open_title(zim)
            results = search.search("German")
            self.assertGreater(len(results), 0)
            titles = []
            for result in results:
                self.assertIn("german", result.title.lower())
                entry = zim.get_entry_by_full_url(result.full_url)
                self.assertEqual(result.title, entry.title)
                titles.append(result.title)
            self.assertGreater(results.n_estimated, 0)
            self.assertIn("German Climate Consortium", titles)
            self.assertNotIn("Germanic bog sacrifice", titles)  # it's a non-article entry
            # test fetching more results than available
            n = 0
            for result in results.iter_in_range(5, 10**8):
                n += 1
            self.assertLess(n, 10**5)
            search.close()

    def test_xapian_fulltext_search(self):
        """
        Test a simple search using the xapian fulltext index.
        """
        with self.open_zts_zim("nons", "wikipedia_en_climate_change_mini_2024-06.zim") as zim:
            search = XapianSearch.open_fulltext(zim)
            results = search.search("Clean")
            self.assertGreater(len(results), 0)
            titles = []
            for result in results:
                entry = zim.get_entry_by_full_url(result.full_url)
                self.assertEqual(result.title, entry.title)
                titles.append(result.title)
            self.assertGreater(results.n_estimated, 0)
            self.assertGreater(len(titles), 0)
            search.close()

    def test_xapian_search_range(self):
        """
        Test a range-bound search using the L{pyzim.search.XapianSearch}.
        """
        with self.open_zts_zim("nons", "wikipedia_en_climate_change_mini_2024-06.zim") as zim:
            search = XapianSearch.open_title(zim)
            results = search.search("Clean")
            self.assertGreater(len(results), 0)
            titles = []
            for result in results:
                self.assertIn("clean", result.title.lower())
                titles.append(result.title)
            self.assertGreaterEqual(len(titles), 10)
            n = 0
            for result in results.iter_in_range(3, 4):
                n += 1
                self.assertIn(result.title, titles[2:6])
            self.assertEqual(n, 4)
            search.close()
