"""
Tests for L{pyzim.util.translator}.
"""
import unittest

from pyzim.util.translator import ZimTranslator, PathReaderMixIn

from ..base import TestBase


class CustomTranslator(PathReaderMixIn, ZimTranslator):
    """
    A translator for testing.

    This translator converts all titles of C entries to uppercase while
    keeping track of redirects seen and M entries.

    M entries are removed (except the counter).

    @ivar seen_redirects: a list of tuples of (src, target) of encountered redirects
    @type seen_redirects: L{list} of L{tuple} of (L{str}, L{str})
    @ivar seen_metadata: a list of tuples of (key, value) of encountered metadata
    @type seen_metadata: L{list} of L{tuple} of (L{str}, L{str})
    @ivar finalized: True if C{self.finalize()} was called
    @type finalized: L{bool}
    """
    def __init__(self, inpath, outpath):
        PathReaderMixIn.__init__(self, inpath, outpath)
        ZimTranslator.__init__(self)

        self.seen_redirects = []
        self.seen_metadata = []
        self.finalized = False

    def handle_redirect(self, entry):
        self.seen_redirects.append((entry.full_url, entry.follow().full_url))
        ZimTranslator.handle_redirect(self, entry)

    def handle_M(self, item):
        key = item.url
        value = item.blob_source.get_blob().read(4096)
        self.seen_metadata.append((key, value))

    def handle_C(self, item):
        item.title = item.title.upper()
        return item

    def finalize(self):
        assert not self.finalized
        self.finalized = True
        ZimTranslator.finalize(self)


class TranslatorTests(unittest.TestCase, TestBase):
    """
    Tests for L{pyzim.util.translator}.
    """
    def test_translator(self):
        """
        Test L{pyzim.util.translator.ZimTranslator} and L{pyzim.util.translator.PathReaderMixIn}.
        """
        with self.open_zts_small_dir() as zimdir:
            # get original data for comparision
            expected_seen_metadata = []
            expected_seen_redirects = []
            expected_num_entries = 1  # 1 for MCounter
            with zimdir.open(mode="r") as zim:
                mainpage_path = zim.get_mainpage_entry().resolve().full_url
                for entry in zim.iter_entries():
                    expected_num_entries += 1
                    if entry.namespace == "M":
                        expected_seen_metadata.append((entry.url, entry.read()))
                        expected_num_entries -= 1  # will not be included
                    if entry.is_redirect:
                        expected_seen_redirects.append((entry.full_url, entry.follow().full_url))
            # setup translator and translate
            inpath = zimdir.get_full_path()
            outpath = zimdir.get_full_path("out.zim")
            translator = CustomTranslator(inpath, outpath)
            translator.translate()
            # check translator state
            self.assertTrue(translator.finalized)
            self.assertListEqual(
                list(sorted(translator.seen_metadata)),
                list(sorted(expected_seen_metadata)),
            )
            self.assertListEqual(
                list(sorted(translator.seen_redirects)),
                list(sorted(expected_seen_redirects)),
            )
            # now out.zim should contain the translated version
            # this means only uppercase titles and no M namespace entries
            num_entries = 0
            with zimdir.open(outpath, mode="r") as zim:
                self.assertEqual(zim.get_mainpage_entry().resolve().full_url, mainpage_path)
                for entry in zim.iter_entries():
                    num_entries += 1
                    if entry.namespace == "C":
                        self.assertEqual(entry.title, entry.title.upper())
                    if entry.url != "Counter":
                        self.assertNotEqual(entry.namespace, "M")
            self.assertEqual(num_entries, expected_num_entries)
