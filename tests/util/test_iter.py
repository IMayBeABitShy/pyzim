"""
Tests for L{pyzim.util.iter}.
"""
import unittest

from pyzim.util.iter import iter_by_cluster

from ..base import TestBase


class IterTests(unittest.TestCase, TestBase):
    """
    Tests for L{pyzim.util.iter}.
    """
    def test_iter_by_cluster(self):
        """
        Test L{pyzim.util.iter.iter_by_cluster}
        """
        with self.open_zts_small() as zim:
            last_cluster_num = -1
            saw_redirects = False
            for urls in iter_by_cluster(zim):
                self.assertTrue(urls)
                e_0 = zim.get_entry_by_full_url(urls[0])
                if e_0.is_redirect:
                    # tests for redirects
                    self.assertFalse(saw_redirects)
                    for url in urls:
                        entry = zim.get_entry_by_full_url(url)
                        self.assertTrue(entry.is_redirect)
                    saw_redirects = True
                else:
                    self.assertFalse(saw_redirects)  # redirects _after_ content
                    self.assertGreater(e_0.cluster_number, last_cluster_num)
                    last_blob_num = -1
                    for url in urls:
                        entry = zim.get_entry_by_full_url(url)
                        self.assertFalse(entry.is_redirect)
                        self.assertEqual(entry.cluster_number, e_0.cluster_number)
                        self.assertGreater(entry.blob_number, last_blob_num)
                        last_blob_num = entry.blob_number
                    last_cluster_num = e_0.cluster_number
            self.assertTrue(saw_redirects)
        # type error test
        with self.assertRaises(TypeError):
            for url in iter_by_cluster(0):
                pass
