"""
Tests for L{pyzim.compressionstrategy}.
"""
import unittest
from unittest import mock

from pyzim.item import Item
from pyzim.compression import CompressionType
from pyzim.compressionstrategy import BaseCompressionStrategy, SimpleCompressionStrategy, MimetypeBasedCompressionStrategy
from pyzim.blob import InMemoryBlobSource, EmptyBlobSource

from .base import TestBase


class BaseCompressionStrategyTests(unittest.TestCase, TestBase):
    """
    Specific tests for L{pyzim.compressionstrategy.BaseCompressionStrategy}.
    """
    def test_notimplemented(self):
        """
        Ensure the default implementations raise a L{NotImplementedError}.
        """
        with self.open_zts_small() as zim:
            item = Item("C", "test/url", "testmime", EmptyBlobSource(), "testtitle")
            strategy = BaseCompressionStrategy(zim)
            with self.assertRaises(NotImplementedError):
                strategy.add_item(item)
            with self.assertRaises(NotImplementedError):
                strategy.flush()
            with self.assertRaises(NotImplementedError):
                strategy.has_items()


class CompressionStrategyTestBase(unittest.TestCase, TestBase):
    """
    Base tests for all L{pyzim.compressionstrategy.BaseCompressionStrategy} implementations.
    """
    def get_strategy_class_kwargs(self):
        """
        Return the compression strategy class and kwargs.

        This should be overwritten in the various specific test cases.

        @return: a tuple of (class, kwargs)
        @rtype: L{tuple} of (class, L{dict})
        """
        return (SimpleCompressionStrategy, {"compression_type": CompressionType.NONE})

    def get_strategy(self, zim):
        """
        Instantiate a new compression strategy to test and return it.

        @param zim: zim archive in which the strategy should write
        @type zim: L{pyzim.archive.Zim}
        @return: the compression strategy that should be tested
        @rtype: L{pyzim.compressionstrategy.BaseCompressionStrategy}
        """
        cls, kwargs = self.get_strategy_class_kwargs()
        return cls(zim, **kwargs)

    def get_item(self, size, url="test/url", mimetype="testmime"):
        """
        Return an item with the specified content size.

        @param size: size for the blob of this item
        @type size: L{int}
        @param url: url of the item
        @type url: L{str}
        @param mimetype: mimetype of the time
        @type mimetype: L{str}
        @return: an item
        @rtype: L{pyzim.item.Item}
        """
        assert isinstance(size, int) and size >= 0
        assert isinstance(url, str)
        assert isinstance(mimetype, str)
        content = b"t" * size
        blob_source = InMemoryBlobSource(content)
        item = Item(
            namespace="C",
            url=url,
            mimetype=mimetype,
            blob_source=blob_source,
            title="Test Title",
        )
        return item

    def test_item_flush(self):
        """
        Test that an item will have been written after flush.
        """
        with self.open_zts_small_dir() as zimdir:
            with zimdir.open(mode="u") as zim:
                # mock zim
                cluster_number = 7
                zim.write_entry = mock.MagicMock()
                zim.write_cluster = mock.MagicMock(return_value=cluster_number)
                # setup strategy and add an item and flush
                strategy = self.get_strategy(zim)
                # check that the strategy does not have any initial items
                self.assertFalse(strategy.has_items())
                size = 1024
                url = "test/url"
                item = self.get_item(size, url=url)
                strategy.add_item(item)
                # check that the strategy does have unwritten items
                self.assertTrue(strategy.has_items())
                strategy.flush()
                # ensure mocks were called
                cluster = zim.write_cluster.call_args[0][0]
                self.assertEqual(cluster.get_number_of_blobs(), 1)
                self.assertEqual(cluster.get_blob_size(0), size)
                entry = zim.write_entry.call_args[0][0]
                self.assertEqual(entry.url, url)
                self.assertEqual(entry.cluster_number, cluster_number)
                self.assertEqual(entry.blob_number, 0)
                # check that the strategy does not have any items after flush
                self.assertFalse(strategy.has_items())


class SimpleCompressionStrategyTests(CompressionStrategyTestBase):
    """
    Tests for L{pyzim.compressionstrategy.SimpleCompressionStrategy}.
    """
    def get_strategy_class_kwargs(self):
        return (SimpleCompressionStrategy, {"compression_type": CompressionType.ZLIB, "max_size": 1024})

    def test_proper_compression(self):
        """
        Test that the created cluster uses the right compression type.
        """
        with self.open_zts_small_dir() as zimdir:
            with zimdir.open(mode="u") as zim:
                # mock zim
                cluster_number = 7
                zim.write_cluster = mock.MagicMock(return_value=cluster_number)
                zim.write_entry = mock.MagicMock()
                # setup strategy and add an item and flush
                strategy = self.get_strategy(zim)
                size = 16
                url = "test/url"
                item = self.get_item(size, url=url)
                strategy.add_item(item)
                strategy.flush()
                # ensure mocks were called
                cluster = zim.write_cluster.call_args[0][0]
                self.assertEqual(cluster.get_number_of_blobs(), 1)
                self.assertEqual(cluster.get_blob_size(0), size)
                self.assertEqual(cluster.compression, CompressionType.ZLIB)

    def test_new_cluster(self):
        """
        Test that new clusters are only created when the size requires it.
        """
        with self.open_zts_small_dir() as zimdir:
            with zimdir.open(mode="u") as zim:
                # mock zim
                cluster_number = 7
                zim.write_entry = mock.MagicMock()
                zim.write_cluster = mock.MagicMock(return_value=cluster_number)
                # write a couple of small items
                strategy = self.get_strategy(zim)
                num_small = 8
                for i in range(num_small):
                    size = 16
                    url = "test/url"
                    item = self.get_item(size, url=url)
                    strategy.add_item(item)
                    zim.write_cluster.assert_not_called()
                # add a big item
                # this should flush the previous cluster
                item = self.get_item(1024 + 512, url="big/item")
                strategy.add_item(item)
                zim.write_cluster.assert_called()
                cluster = zim.write_cluster.call_args[0][0]
                self.assertEqual(cluster.get_number_of_blobs(), num_small)
                zim.write_cluster.reset_mock()
                # adding another small item would again flush the previous big item
                for i in range(2):
                    item = self.get_item(32, url="big/item")
                    strategy.add_item(item)
                zim.write_cluster.assert_called()
                cluster = zim.write_cluster.call_args[0][0]
                self.assertEqual(cluster.get_number_of_blobs(), 1)
                zim.write_cluster.reset_mock()
                # short clean up
                strategy.flush()
                # adding a single big item in excess capacity
                item = self.get_item(8192, url="big/item")
                strategy.add_item(item)
                # and another so that the previous one gets flushed
                item = self.get_item(8192, url="big/item")
                strategy.add_item(item)
                zim.write_cluster.assert_called()
                cluster = zim.write_cluster.call_args[0][0]
                self.assertEqual(cluster.get_number_of_blobs(), 1)
                zim.write_cluster.reset_mock()
                strategy.flush()
                zim.write_cluster.assert_called()
                zim.write_cluster.reset_mock()
                # test that a single item would bring it over capacity
                to_add = (1024 // (16 + 4)) - 1  # 4 for the offset
                for i in range(to_add):
                    item = self.get_item(16)
                    strategy.add_item(item)
                zim.write_cluster.assert_not_called()
                item = self.get_item(32)
                strategy.add_item(item)
                zim.write_cluster.assert_called()
                cluster = zim.write_cluster.call_args[0][0]
                self.assertEqual(cluster.get_number_of_blobs(), to_add)
                zim.write_cluster.reset_mock()
                strategy.flush()
                zim.write_cluster.assert_called()
                cluster = zim.write_cluster.call_args[0][0]
                self.assertEqual(cluster.get_number_of_blobs(), 1)


class MimetypeBasedCompressionStrategyTests(CompressionStrategyTestBase):
    """
    Tests for L{pyzim.compressionstrategy.MimetypeBasedCompressionStrategy}.
    """
    def get_strategy_class_kwargs(self):
        return (
            MimetypeBasedCompressionStrategy,
            {
                "cs_class": SimpleCompressionStrategy,
                "cs_kwargs": {
                    "compression_type": CompressionType.ZLIB,
                    "max_size": 1024,
                },
            }
        )

    def test_mimetype_assignment(self):
        """
        Test that items are assigned to the proper mimetypes.
        """
        with self.open_zts_small_dir() as zimdir:
            with zimdir.open(mode="u") as zim:
                # setup mock
                zim.write_cluster = mock.MagicMock()
                zim.write_cluster.side_effect = [7, 8]
                zim.write_entry = mock.MagicMock()
                # setup content
                strategy = self.get_strategy(zim)
                item_a_1 = self.get_item(8, mimetype="a")
                item_a_2 = self.get_item(8, mimetype="a")
                item_a_3 = self.get_item(8, mimetype="a")
                item_b_1 = self.get_item(8, mimetype="b")
                item_b_2 = self.get_item(8, mimetype="b")
                item_b_3 = self.get_item(8, mimetype="b")
                # add items
                strategy.add_item(item_a_1)
                strategy.add_item(item_b_1)
                strategy.add_item(item_a_2)
                strategy.add_item(item_a_3)
                strategy.add_item(item_b_2)
                strategy.add_item(item_b_3)
                # finalize
                strategy.flush()
                # ensure clusters were written correctly
                self.assertEqual(zim.write_cluster.call_count, 2)
                # ensure all entries point to the same cluster depending
                # on mimetype
                self.assertEqual(zim.write_entry.call_count, 6)
                all_call_args = zim.write_entry.call_args_list
                entries = [e[0][0] for e in all_call_args]
                mimetypes2cluster_num = {}
                for entry in entries:
                    mt = entry.mimetype
                    if mt in mimetypes2cluster_num:
                        self.assertEqual(mimetypes2cluster_num[mt], entry.cluster_number)
                    else:
                        mimetypes2cluster_num[mt] = entry.cluster_number
                self.assertEqual(len(mimetypes2cluster_num.keys()), 2)
