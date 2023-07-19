"""
Tests for L{pyzim.cluster}.
"""
import io
import struct
import unittest
from unittest import mock

from pyzim import constants, exceptions
from pyzim.cluster import Cluster, OffsetRememberingCluster, InMemoryCluster
from pyzim.compression import CompressionType
from pyzim.policy import Policy

from .base import TestBase


class ClusterTests(unittest.TestCase, TestBase):
    """
    Tests for L{pyzim.cluster.Cluster}.
    """
    cluster_class = Cluster

    def get_policy(self):
        """
        Helper function that returns a policy to use.
        """
        return Policy(cluster_class=self.cluster_class)

    def test_init(self):
        """
        Test L{pyzim.cluster.Cluster.__init__}.
        """
        cluster = self.cluster_class()
        # on init, compression and extension remain unknown
        self.assertIsNone(cluster.compression)
        self.assertIsNone(cluster.is_extended)
        self.assertIsNone(cluster.offset)
        # check error if only offset but not zim is specified
        with self.assertRaises(ValueError):
            self.cluster_class(offset=3)

    def test_pointer_format(self):
        """
        Test L{pyzim.cluster.Cluster._pointer_format}.
        """
        cluster = self.cluster_class()
        # without reading the infobyte, the pointer format is not available
        with self.assertRaises(RuntimeError):
            cluster._pointer_format
        # manually set cluster extension
        cluster.is_extended = False
        self.assertEqual(cluster._pointer_format, constants.ENDIAN + "I")
        cluster.is_extended = True
        self.assertEqual(cluster._pointer_format, constants.ENDIAN + "Q")

    def test_infobyte(self):
        """
        Test L{pyzim.cluster.Cluster.read_infobyte} and L{pyzim.cluster.Cluster.generate_infobyte}.
        """
        cluster = self.cluster_class()
        self.assertFalse(cluster.did_read_infobyte)
        # without being bound, reading the infobyte should not be possible
        with self.assertRaises(exceptions.BindRequired):
            cluster.read_infobyte()
        with self.assertRaises(exceptions.BindRequired):
            cluster.read_infobyte_if_needed()

        # continue tests with a bound cluster
        with self.open_zts_small(policy=self.get_policy()) as zim:
            cluster = zim.get_cluster_by_index(0)
            cluster.read_infobyte_if_needed()
            self.assertIsNotNone(cluster.is_extended)
            self.assertIsNotNone(cluster.compression)

            # assert error on empty read
            # we just skip to the end for this
            cluster_2 = zim.get_cluster_by_index(0)
            with zim.acquire_file() as f:
                f.seek(0, io.SEEK_END)
                cluster_2.offset = f.tell()
            with self.assertRaises(IOError):
                cluster_2.read_infobyte()

        # test parsing of infobyte
        infobyte_values = (
            # (byte (int), is_extended, compression)
            # without extension
            (0b00000001, False, CompressionType.NONE),
            (0b00000010, False, CompressionType.ZLIB),
            (0b00000011, False, CompressionType.BZ2),
            (0b00000100, False, CompressionType.LZMA2),
            (0b00000101, False, CompressionType.ZSTD),
            # with extension
            (0b00010001, True, CompressionType.NONE),
            (0b00010010, True, CompressionType.ZLIB),
            (0b00010011, True, CompressionType.BZ2),
            (0b00010100, True, CompressionType.LZMA2),
            (0b00010101, True, CompressionType.ZSTD),
        )
        for raw_infobyte, is_extended, compression in infobyte_values:
            infobyte = chr(raw_infobyte).encode("ASCII")
            cluster = self.cluster_class()
            cluster.parse_infobyte(infobyte)
            self.assertEqual(cluster.is_extended, is_extended)
            self.assertEqual(cluster.compression, compression)
            generated_infobyte = cluster.generate_infobyte()
            self.assertEqual(generated_infobyte, infobyte)

        # check error on unknown compression
        with self.assertRaises(exceptions.UnsupportedCompressionType):
            infobyte = chr(0b00001110).encode("ASCII")
            cluster = self.cluster_class()
            cluster.parse_infobyte(infobyte)

    def test_iter_blob_offsets(self):
        """
        Test L{pyzim.cluster.Cluster.iter_blob_offsets}.
        """
        # test fail on unbound cluster
        cluster = self.cluster_class()
        with self.assertRaises(exceptions.BindRequired):
            [o for o in cluster.iter_blob_offsets()]

        # continue test with bound cluster
        with self.open_zts_small(policy=self.get_policy()) as zim:
            cluster = zim.get_cluster_by_index(0)
            all_offsets = [o for o in cluster.iter_blob_offsets()]
            self.assertGreater(len(all_offsets), 2)

            # check call can be repeated
            all_offsets_2 = [o for o in cluster.iter_blob_offsets()]
            self.assertEqual(all_offsets, all_offsets_2)

            # check call returns empty if blob_numbers are empty
            self.assertEqual(
                [o for o in cluster.iter_blob_offsets(blob_numbers=[])],
                [],
            )

            # check call works for specific blob numbers
            self.assertEqual(
                [o for o in cluster.iter_blob_offsets(blob_numbers=[1, 2])],
                all_offsets[1:3],  # 3 because slices are end-exclusive
            )
            # check blob_number order does not matter
            self.assertEqual(
                [o for o in cluster.iter_blob_offsets(blob_numbers=[2, 0, 1])],
                all_offsets[0:3],  # 3 because slices are end-exclusive
            )

    def test_get_offset(self):
        """
        Test L{pyzim.cluster.Cluster.get_offset}.
        """
        # test fail on unbound cluster
        cluster = self.cluster_class()
        with self.assertRaises(exceptions.BindRequired):
            cluster.get_offset(0)

        # continue test with bound cluster
        with self.open_zts_small(policy=self.get_policy()) as zim:
            cluster = zim.get_cluster_by_index(0)
            all_offsets = [o for o in cluster.iter_blob_offsets()]
            self.assertGreater(len(all_offsets), 2)

            for i in range(len(all_offsets)):
                offset_a = all_offsets[i]
                offset_b = cluster.get_offset(i)
                self.assertEqual(offset_a, offset_b)

            # assert IndexError on high index
            with self.assertRaises(IndexError):
                cluster.get_offset(len(all_offsets))
            with self.assertRaises(IndexError):
                cluster.get_offset(-1)

    def test_get_number_of_offsets(self):
        """
        Test L{pyzim.cluster.Cluster.get_number_of_offsets}.
        """
        # test fail on unbound cluster
        cluster = self.cluster_class()
        with self.assertRaises(exceptions.BindRequired):
            cluster.get_number_of_offsets()

        # continue test with bound cluster
        with self.open_zts_small(policy=self.get_policy()) as zim:
            cluster = zim.get_cluster_by_index(0)
            all_offsets = [o for o in cluster.iter_blob_offsets()]
            self.assertEqual(cluster.get_number_of_offsets(), len(all_offsets))

    def test_get_number_of_blobs(self):
        """
        Test L{pyzim.cluster.Cluster.get_number_of_blobs}.
        """
        # test fail on unbound cluster
        cluster = self.cluster_class()
        with self.assertRaises(exceptions.BindRequired):
            cluster.get_number_of_blobs()

        # continue test with bound cluster
        with self.open_zts_small(policy=self.get_policy()) as zim:
            cluster = zim.get_cluster_by_index(0)
            num_offsets = cluster.get_number_of_offsets()  # tested separately
            num_blobs = cluster.get_number_of_blobs()
            self.assertGreater(num_blobs, 0)
            self.assertLess(num_blobs, num_offsets)

    def test_get_content_size(self):
        """
        Test L{pyzim.cluster.Cluster.get_content_size}.
        """
        # check error on unbound cluster
        cluster = self.cluster_class()
        with self.assertRaises(exceptions.BindRequired):
            cluster.get_content_size()
        # TODO: check with custom offsets -> patching?
        # check size equals total blob size
        with self.open_zts_small(policy=self.get_policy()) as zim:
            cluster = zim.get_cluster_by_index(0)
            total_size = 0
            for i in range(cluster.get_number_of_blobs()):
                blob_size = cluster.get_blob_size(i)
                self.assertGreaterEqual(blob_size, 0)
                total_size += blob_size
            self.assertEqual(total_size, cluster.get_content_size())
            self.assertGreater(total_size, 0)
            self.assertLess(total_size, cluster.get_total_decompressed_size())

    def test_get_total_decompressed_size(self):
        """
        Test L{pyzim.cluster.Cluster.get_total_decompressed_size}.
        """
        # check error on unbound cluster
        cluster = self.cluster_class()
        with self.assertRaises(exceptions.BindRequired):
            cluster.get_total_decompressed_size()
        # TODO: check with custom offsets -> patching?
        with self.open_zts_small(policy=self.get_policy()) as zim:
            cluster = zim.get_cluster_by_index(0)
            decompressed_size = cluster.get_total_decompressed_size()
            n_offsets = cluster.get_number_of_offsets()
            offset_size = struct.calcsize(cluster._pointer_format)
            self.assertEqual(decompressed_size, (n_offsets * offset_size) + cluster.get_content_size())
            self.assertGreater(decompressed_size, 0)
            self.assertLess(cluster.get_content_size(), cluster.get_total_decompressed_size())

    def test_get_total_offset_size(self):
        """
        Test L{pyzim.cluster.Cluster.get_total_offset_size}.
        """
        # check error on unbound cluster
        cluster = self.cluster_class()
        with self.assertRaises(exceptions.BindRequired):
            cluster.get_total_offset_size()
        # TODO: check with custom offsets -> patching?
        with self.open_zts_small(policy=self.get_policy()) as zim:
            cluster = zim.get_cluster_by_index(0)
            offset_size = cluster.get_total_offset_size()
            n_offsets = cluster.get_number_of_offsets()
            calculated_offset_size = struct.calcsize(cluster._pointer_format)
            self.assertEqual(offset_size, (n_offsets * calculated_offset_size))
            self.assertGreater(offset_size, 3)  # at least 1 offset of at least 4 bytes
            self.assertLess(offset_size, cluster.get_total_decompressed_size())

    def test_get_blob_size(self):
        """
        Test L{pyzim.cluster.Cluster.get_blob_size}.
        """
        # check error on unbound cluster
        cluster = self.cluster_class()
        with self.assertRaises(exceptions.BindRequired):
            cluster.get_blob_size(0)
        # TODO: check with custom offsets -> patching?
        with self.open_zts_small(policy=self.get_policy()) as zim:
            cluster = zim.get_cluster_by_index(0)
            for i in range(cluster.get_number_of_blobs()):
                blob_size = cluster.get_blob_size(i)
                self.assertGreaterEqual(blob_size, 0)
                self.assertEqual(blob_size, len(cluster.read_blob(i)))

    def test_read_blob(self):
        """
        Test L{pyzim.cluster.Cluster.read_blob}.
        """
        # check error on unbound cluster
        cluster = self.cluster_class()
        with self.assertRaises(exceptions.BindRequired):
            cluster.read_blob(0)
        # check size of read equals blob size
        with self.open_zts_small(policy=self.get_policy()) as zim:
            cluster = zim.get_cluster_by_index(0)
            for i in range(cluster.get_number_of_blobs()):
                blob_size = cluster.get_blob_size(i)
                blob_content = cluster.read_blob(i)
                self.assertGreaterEqual(len(blob_content), 0)
                self.assertEqual(blob_size, len(blob_content))

    def test_iter_read_blob(self):
        """
        Test L{pyzim.cluster.Cluster.iter_read_blob}.
        """
        # check error on unbound cluster
        cluster = self.cluster_class()
        with self.assertRaises(exceptions.BindRequired):
            for chunk in cluster.iter_read_blob(0):
                pass
        # check size of read equals blob size
        with self.open_zts_small(policy=self.get_policy()) as zim:
            cluster = zim.get_cluster_by_index(0)
            for i in range(cluster.get_number_of_blobs()):
                blob_size = cluster.get_blob_size(i)
                blob_content = b""
                buffersize = 3
                for chunk in cluster.iter_read_blob(i, buffersize=buffersize):
                    self.assertLessEqual(len(chunk), buffersize)
                    blob_content += chunk
                self.assertEqual(blob_size, len(blob_content))
                self.assertEqual(blob_content, cluster.read_blob(i))

    def test_reader_recycling(self):
        """
        Test reader recycling behavior.
        """
        cluster = self.cluster_class()
        cluster.is_extended = False
        cluster.compression = CompressionType.NONE
        raw_data = cluster.generate_infobyte() + b"0123456789"
        f = io.BytesIO(raw_data)

        with self.open_zts_small() as zim:
            fmock = mock.MagicMock()
            fmock.return_value.__enter__.return_value = f
            zim.acquire_file = fmock
            cluster.bind(zim)
            cluster.offset = 0
            with cluster._get_decompressing_reader(offset=1) as reader:
                reader_1 = reader
                self.assertEqual(reader.read(2), b"12")
            # read later, should be recycled
            with cluster._get_decompressing_reader(offset=4) as reader:
                self.assertEqual(reader.read(2), b"45")
                self.assertIs(reader, reader_1)
            # read directly following, should be recycled
            with cluster._get_decompressing_reader(offset=6) as reader:
                self.assertEqual(reader.read(2), b"67")
                self.assertIs(reader, reader_1)
            # read earlier, should not be recycled
            with cluster._get_decompressing_reader(offset=3) as reader:
                reader_2 = reader
                self.assertEqual(reader.read(2), b"34")
                self.assertIsNot(reader, reader_1)
            # reader overlapping, should not be recycled
            with cluster._get_decompressing_reader(offset=4) as reader:
                self.assertEqual(reader.read(2), b"45")
                reader_3 = reader
                self.assertIsNot(reader, reader_2)
                self.assertIsNot(reader, reader_1)
            # reader directly following, should be recycled
            with cluster._get_decompressing_reader(offset=6) as reader:
                self.assertEqual(reader.read(2), b"67")
                self.assertIs(reader, reader_3)


class OffsetRememberingClusterTests(ClusterTests):
    """
    Tests for L{pyzim.cluster.OffsetRememberingCluster}.
    """
    cluster_class = OffsetRememberingCluster


class InMemoryClusterTests(ClusterTests):
    """
    Tests for L{pyzim.cluster.InMemoryCluster}.
    """
    cluster_class = InMemoryCluster


class ClusterBehaviorEquivalenceTests(unittest.TestCase, TestBase):
    """
    Tests to ensure that cluster class behavior matches.
    """
    cluster_classes = [
        Cluster,
        OffsetRememberingCluster,
        InMemoryCluster,
    ]

    def test_behavior(self):
        """
        Test that the varios classes behave equally.
        """
        # test strategy:
        # for all clusters in ZTS-small:
        # - read various metrics of the cluster with any cluster class
        # - repeat with other cluster classes, asserting equivalence to
        # previous result

        with self.open_zts_small() as zim:
            for cluster_pos in zim._cluster_pointer_list.iter_pointers():
                print("===== next cluster =====")
                cmp_attributes = None
                cmp_counts = None
                cmp_sizes = None
                cmp_offsets = None
                cmp_blobs = None
                for cluster_class in self.cluster_classes:
                    print("Class: {}".format(repr(cluster_class)))
                    cluster = cluster_class(zim, cluster_pos)
                    cluster.read_infobyte_if_needed()

                    attributes = (
                        cluster.is_extended,
                        cluster.compression,
                    )
                    counts = (
                        cluster.get_number_of_offsets(),
                        cluster.get_number_of_blobs(),
                    )
                    sizes = (
                        cluster.get_content_size(),
                        cluster.get_total_decompressed_size(),
                        cluster.get_total_offset_size(),
                    )
                    offsets = [offset for offset in cluster.iter_blob_offsets()]
                    blobs = [cluster.read_blob(i) for i in range(cluster.get_number_of_blobs())]

                    if cmp_attributes is None:
                        cmp_attributes = attributes
                    else:
                        self.assertEqual(cmp_attributes, attributes)
                    if cmp_counts is None:
                        cmp_counts = counts
                    else:
                        self.assertEqual(cmp_counts, counts)
                    if cmp_sizes is None:
                        cmp_sizes = sizes
                    else:
                        self.assertEqual(cmp_sizes, sizes)
                    if cmp_offsets is None:
                        cmp_offsets = offsets
                    else:
                        self.assertEqual(cmp_offsets, offsets)
                    if cmp_blobs is None:
                        cmp_blobs = blobs
                    else:
                        # checking blob equivalence at once may make the
                        # console output unreadable
                        # instead, check individually
                        for blob_a, blob_b in zip(cmp_blobs, blobs):
                            self.assertEqual(blob_a, blob_b)
