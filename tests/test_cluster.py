"""
Tests for L{pyzim.cluster}.
"""
import io
import struct
import unittest
from unittest import mock

from pyzim import constants, exceptions
from pyzim.blob import InMemoryBlobSource
from pyzim.cluster import Cluster, OffsetRememberingCluster, InMemoryCluster, ModifiableClusterWrapper, EmptyCluster
from pyzim.compression import CompressionType
from pyzim.policy import Policy

from .base import TestBase


class ModifiableClusterWrapperHelper(ModifiableClusterWrapper):
    """
    A helper class for testing L{ModifiableClusterWrapper}.

    This class changes the constructor of L{ModifiableClusterWrapper} so
    that it takes the arguments and construct a L{Cluster} that will then
    be wrapped by this class.
    """
    def __init__(self, *args, **kwargs):
        """
        The default constructor.

        Initializes L{ModifiableClusterWrapper} with a new L{Cluster}.

        @param args: positional arguments passed to L{Cluster.__init__}
        @param kwargs: keyword arguments passed to L{Cluster.__init__}
        """
        ModifiableClusterWrapper.__init__(self, Cluster(*args, **kwargs))


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

    def test_get_total_compressed_size(self):
        """
        Test L{pyzim.cluster.Cluster.get_total_compressed_size}
        """
        # check error on unbound cluster
        cluster = self.cluster_class()
        with self.assertRaises(exceptions.BindRequired):
            cluster.get_total_compressed_size()
        with self.open_zts_small(policy=self.get_policy()) as zim:
            for cluster in zim.iter_clusters():
                # get the size of a cluster as well as the last blob
                n_blobs = cluster.get_number_of_blobs()
                last_blob_data = cluster.read_blob(n_blobs - 1)
                cluster_size = cluster.get_total_compressed_size()
                # extract the cluster to check that the size is correct
                cluster_start = cluster.offset
                with zim.acquire_file() as f:
                    f.seek(cluster_start)
                    cluster_data = b""
                    while len(cluster_data) < cluster_size:
                        cluster_data += f.read(cluster_size - len(cluster_data))
                cluster_f = io.BytesIO(cluster_data)
                # mock the file acquisition, so that we use cluster_f
                org_method = zim.acquire_file
                fmock = mock.MagicMock()
                fmock.return_value.__enter__.return_value = cluster_f
                zim.acquire_file = fmock
                cluster_2 = Cluster(zim=zim, offset=0)
                copied_blob_data = cluster_2.read_blob(n_blobs - 1)
                self.assertEqual(copied_blob_data, last_blob_data)
                zim.acquire_file = org_method

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
            # check error if blob does not exists
            with self.assertRaises(exceptions.BlobNotFound):
                cluster.read_blob(cluster.get_number_of_blobs())

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
            # check error on reading out of range
            with self.assertRaises(exceptions.BlobNotFound):
                for chunk in cluster.iter_read_blob(cluster.get_number_of_blobs()):
                    pass

    def test_reader_recycling(self):
        """
        Test reader recycling behavior.
        """
        cluster = self.cluster_class()
        cluster.is_extended = False
        cluster.compression = CompressionType.NONE
        raw_data = cluster.generate_infobyte() + b"0123456789"
        f = io.BytesIO(raw_data)

        with self.open_zts_small(policy=self.get_policy()) as zim:
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

    def test_reset_base(self):
        """
        Test L{pyzim.cluster.Cluster.reset}.
        """
        with self.open_zts_small(policy=self.get_policy()) as zim:
            cluster = zim.get_cluster_by_index(0)
            # NOTE: the following asserts have been disabled because the
            # counter may interact with cluster 0
            # self.assertIsNone(cluster.compression)
            # self.assertIsNone(cluster.is_extended)
            # self.assertFalse(cluster.did_read_infobyte)
            cluster.read_blob(0)
            self.assertIsNotNone(cluster.compression)
            self.assertIsNotNone(cluster.is_extended)
            cluster.reset()
            self.assertIsNone(cluster.compression)
            self.assertIsNone(cluster.is_extended)
            self.assertFalse(cluster.did_read_infobyte)


class OffsetRememberingClusterTests(ClusterTests):
    """
    Tests for L{pyzim.cluster.OffsetRememberingCluster}.
    """
    cluster_class = OffsetRememberingCluster

    def test_reset_offsets(self):
        """
        Test L{pyzim.cluster.OffsetRememberingCluster.reset}.
        """
        with self.open_zts_small(policy=self.get_policy()) as zim:
            cluster = zim.get_cluster_by_index(0)
            # NOTE: the following asserts have been disabled because the
            # counter may interact with cluster 0
            # self.assertIsNone(cluster._offsets)
            cluster.read_blob(0)
            self.assertIsNotNone(cluster._offsets)
            cluster.reset()
            self.assertIsNone(cluster._offsets)


class InMemoryClusterTests(OffsetRememberingClusterTests):
    """
    Tests for L{pyzim.cluster.InMemoryCluster}.
    """
    cluster_class = InMemoryCluster

    def test_reset_data(self):
        """
        Test L{pyzim.cluster.InMemoryCluster.reset}.
        """
        with self.open_zts_small(policy=self.get_policy()) as zim:
            cluster = zim.get_cluster_by_index(0)
            # NOTE: the following asserts have been disabled because the
            # counter may interact with cluster 0
            # self.assertIsNone(cluster._data)
            cluster.read_blob(0)
            self.assertIsNotNone(cluster._data)
            cluster.reset()
            self.assertIsNone(cluster._data)


class ModifiableClusterWrapperTests(ClusterTests):
    """
    Tests for L{pyzim.cluster.ModifiableClusterWrapper}.
    """
    cluster_class = ModifiableClusterWrapperHelper

    def get_data_in_cluster(self, cluster):
        """
        Helper method returning a list of bytestring data in a cluster.

        @param cluster: cluster to get data from
        @type cluster: L{pyzim.cluster.Cluster}
        @return: a list of blobs in this cluster
        @rtype: L{list} of L{bytes}
        """
        ret = []
        for i in range(cluster.get_number_of_blobs()):
            ret.append(cluster.read_blob(i))
        return ret

    def test_append_cluster(self):
        """
        Test L{pyzim.cluster.ModifiableClusterWrapper.append}
        """
        with self.open_zts_small_dir() as zimdir:
            with zimdir.open(mode="u") as zim:
                cluster = zim.get_cluster_by_index(0)
                self.assertIsInstance(cluster, ModifiableClusterWrapper)
                old_num_blobs = cluster.get_number_of_blobs()
                unmodified_data = self.get_data_in_cluster(cluster)
                new_blob = InMemoryBlobSource(b"test")
                new_blob_index = cluster.append_blob(new_blob)
                self.assertEqual(new_blob_index, old_num_blobs)
                new_num_blobs = cluster.get_number_of_blobs()
                self.assertEqual(new_num_blobs, old_num_blobs + 1)
                self.assertEqual(cluster.read_blob(new_blob_index), b"test")
                cluster.flush()
            # check that the change was written correctly
            with zimdir.open(mode="r") as zim:
                cluster = zim.get_cluster_by_index(0)
                self.assertEqual(cluster.read_blob(new_blob_index), b"test")
                modified_data = self.get_data_in_cluster(cluster)
                self.assertEqual(unmodified_data, modified_data[:-1])

    def test_append_new_cluster(self):
        """
        Test L{pyzim.cluster.ModifiableClusterWrapper.append} with a new cluster.
        """
        with self.open_zts_small_dir() as zimdir:
            with zimdir.open(mode="u") as zim:
                cluster = zim.new_cluster()
                self.assertIsInstance(cluster, ModifiableClusterWrapper)
                new_blob = InMemoryBlobSource(b"test")
                new_blob_index = cluster.append_blob(new_blob)
                self.assertEqual(new_blob_index, 0)
                num_blobs = cluster.get_number_of_blobs()
                self.assertEqual(num_blobs, 1)
                self.assertEqual(cluster.read_blob(new_blob_index), b"test")
                cluster_num = zim.write_cluster(cluster)
                cluster.flush()
            # check that the change was written correctly
            with zimdir.open(mode="r") as zim:
                cluster = zim.get_cluster_by_index(cluster_num)
                self.assertEqual(cluster.read_blob(new_blob_index), b"test")

    def test_remove_blob(self):
        """
        Test L{pyzim.cluster.ModifiableClusterWrapper.remove_blob}
        """
        with self.open_zts_small_dir() as zimdir:
            with zimdir.open(mode="u") as zim:
                cluster = zim.get_cluster_by_index(0)
                self.assertIsInstance(cluster, ModifiableClusterWrapper)
                old_num_blobs = cluster.get_number_of_blobs()
                unmodified_data = self.get_data_in_cluster(cluster)
                del unmodified_data[3]
                blob_4 = cluster.read_blob(4)
                cluster.remove_blob(3)
                new_num_blobs = cluster.get_number_of_blobs()
                self.assertEqual(new_num_blobs, old_num_blobs - 1)
                self.assertEqual(cluster.read_blob(3), blob_4)
                cluster.flush()
            # check that the change was written correctly
            with zimdir.open(mode="r") as zim:
                cluster = zim.get_cluster_by_index(0)
                self.assertEqual(cluster.read_blob(3), blob_4)
                modified_data = self.get_data_in_cluster(cluster)
                self.assertEqual(unmodified_data, modified_data)
        # repeat test, this time removing an appended blob
        with self.open_zts_small_dir() as zimdir:
            with zimdir.open(mode="u") as zim:
                cluster = zim.get_cluster_by_index(0)
                self.assertIsInstance(cluster, ModifiableClusterWrapper)
                old_num_blobs = cluster.get_number_of_blobs()
                unmodified_data = self.get_data_in_cluster(cluster)
                new_blob = InMemoryBlobSource(b"test")
                new_blob_index = cluster.append_blob(new_blob)
                self.assertEqual(new_blob_index, old_num_blobs)
                new_num_blobs = cluster.get_number_of_blobs()
                self.assertEqual(new_num_blobs, old_num_blobs + 1)
                self.assertEqual(cluster.read_blob(new_blob_index), b"test")
                cluster.remove_blob(new_blob_index)
                self.assertEqual(cluster.get_number_of_blobs(), old_num_blobs)
                cluster.flush()
            # check that the change was written correctly
            with zimdir.open(mode="r") as zim:
                cluster = zim.get_cluster_by_index(0)
                with self.assertRaises(exceptions.BlobNotFound):
                    # blob index should no longer exists
                    cluster.read_blob(new_blob_index)
                modified_data = self.get_data_in_cluster(cluster)
                self.assertEqual(unmodified_data, modified_data)

    def test_empty_blob(self):
        """
        Test L{pyzim.cluster.ModifiableClusterWrapper.empty_blob}
        """
        with self.open_zts_small_dir() as zimdir:
            with zimdir.open(mode="u") as zim:
                cluster = zim.get_cluster_by_index(0)
                self.assertIsInstance(cluster, ModifiableClusterWrapper)
                old_num_blobs = cluster.get_number_of_blobs()
                unmodified_data = self.get_data_in_cluster(cluster)
                # as part of this test, we also delete another blob
                # so that we can test index adjustment
                cluster.remove_blob(2)
                del unmodified_data[2]
                unmodified_data[3] = b""
                cluster.empty_blob(3)
                new_num_blobs = cluster.get_number_of_blobs()
                self.assertEqual(new_num_blobs, old_num_blobs - 1)  # due to deletion
                self.assertEqual(cluster.read_blob(3), b"")
                cluster.flush()
            # check that the change was written correctly
            with zimdir.open(mode="r") as zim:
                cluster = zim.get_cluster_by_index(0)
                self.assertEqual(cluster.read_blob(3), b"")
                modified_data = self.get_data_in_cluster(cluster)
                self.assertEqual(unmodified_data, modified_data)

    def test_set_blob_normal(self):
        """
        Test L{pyzim.cluster.ModifiableClusterWrapper.set_blob} for normal modification.
        """
        with self.open_zts_small_dir() as zimdir:
            with zimdir.open(mode="u") as zim:
                cluster = zim.get_cluster_by_index(0)
                self.assertIsInstance(cluster, ModifiableClusterWrapper)
                old_num_blobs = cluster.get_number_of_blobs()
                unmodified_data = self.get_data_in_cluster(cluster)
                # as part of this test, we also delete another blob
                # so that we can test index adjustment
                cluster.remove_blob(2)
                del unmodified_data[2]
                # set blob
                new_blob = InMemoryBlobSource(b"test")
                cluster.set_blob(3, new_blob)
                unmodified_data[3] = b"test"
                new_num_blobs = cluster.get_number_of_blobs()
                self.assertEqual(new_num_blobs, old_num_blobs - 1)
                self.assertEqual(cluster.read_blob(3), b"test")
                cluster.flush()
            # check that the change was written correctly
            with zimdir.open(mode="r") as zim:
                cluster = zim.get_cluster_by_index(0)
                self.assertEqual(cluster.read_blob(3), b"test")
                modified_data = self.get_data_in_cluster(cluster)
                self.assertEqual(unmodified_data, modified_data)

    def test_set_blob_deleted(self):
        """
        Test L{pyzim.cluster.ModifiableClusterWrapper.set_blob} for setting a deleted blob.
        """
        with self.open_zts_small_dir() as zimdir:
            with zimdir.open(mode="u") as zim:
                cluster = zim.get_cluster_by_index(0)
                self.assertIsInstance(cluster, ModifiableClusterWrapper)
                old_num_blobs = cluster.get_number_of_blobs()
                unmodified_data = self.get_data_in_cluster(cluster)
                # as part of this test, we deletee two blobs, one to
                # test replacement and one to test index adjustment
                cluster.remove_blob(2)
                del unmodified_data[2]
                cluster.remove_blob(2)
                unmodified_data[2] = b"test"
                # set blob
                new_blob = InMemoryBlobSource(b"test")
                cluster.set_blob(2, new_blob)
                new_num_blobs = cluster.get_number_of_blobs()
                self.assertEqual(new_num_blobs, old_num_blobs - 1)
                self.assertEqual(cluster.read_blob(2), b"test")
                cluster.flush()
            # check that the change was written correctly
            with zimdir.open(mode="r") as zim:
                cluster = zim.get_cluster_by_index(0)
                self.assertEqual(cluster.get_number_of_blobs(), old_num_blobs - 1)
                self.assertEqual(cluster.read_blob(2), b"test")
                modified_data = self.get_data_in_cluster(cluster)
                self.assertEqual(unmodified_data, modified_data)

    def test_set_blob_append(self):
        """
        Test L{pyzim.cluster.ModifiableClusterWrapper.set_blob} for appending.
        """
        with self.open_zts_small_dir() as zimdir:
            with zimdir.open(mode="u") as zim:
                cluster = zim.get_cluster_by_index(0)
                self.assertIsInstance(cluster, ModifiableClusterWrapper)
                old_num_blobs = cluster.get_number_of_blobs()
                unmodified_data = self.get_data_in_cluster(cluster)
                # as part of this test, we also delete another blob
                # so that we can test index adjustment
                cluster.remove_blob(2)
                del unmodified_data[2]
                # set blob
                new_blob = InMemoryBlobSource(b"test")
                new_blob_index = cluster.get_number_of_blobs()
                cluster.set_blob(new_blob_index, new_blob)
                unmodified_data.append(b"test")
                new_num_blobs = cluster.get_number_of_blobs()
                self.assertEqual(new_num_blobs, old_num_blobs)
                self.assertEqual(cluster.read_blob(new_blob_index), b"test")
                cluster.flush()
            # check that the change was written correctly
            with zimdir.open(mode="r") as zim:
                cluster = zim.get_cluster_by_index(0)
                self.assertEqual(cluster.read_blob(new_blob_index), b"test")
                modified_data = self.get_data_in_cluster(cluster)
                self.assertEqual(unmodified_data, modified_data)

    def test_flush(self):
        """
        Test L{pyzim.cluster.ModifiableClusterWrapper.flush}.
        """
        # The test mostly happens during other test cases
        # there, we test that the cluster is written properly
        # here, we only test special cases
        with self.open_zts_small_dir() as zimdir:
            with zimdir.open(mode="u") as zim:
                cluster = zim.get_cluster_by_index(0)
                # test error if not bound
                cluster.unbind()
                with self.assertRaises(exceptions.BindRequired):
                    cluster.flush()
                cluster.bind(zim)
                # test flush if not dirty
                zim.write_cluster = mock.MagicMock()
                cluster.flush()
                zim.write_cluster.assert_not_called()

    def test_compression(self):
        """
        Test L{pyzim.cluster.ModifiableClusterWrapper.compression}.
        """
        with self.open_zts_small_dir() as zimdir:
            with zimdir.open(mode="u") as zim:
                cluster = zim.get_cluster_by_index(0)
                # test TypeError
                with self.assertRaises(TypeError):
                    cluster.compression = "wasd"
                # test unsupported compression
                with self.assertRaises(exceptions.UnsupportedCompressionType):
                    cluster.compression = 700
                # test setting to enum
                cluster.compression = CompressionType.LZMA2
                self.assertEqual(cluster.compression, CompressionType.LZMA2)
                self.assertTrue(cluster.dirty)
                cluster.dirty = False
                # test setting to number
                cluster.compression = CompressionType.ZLIB.value
                self.assertEqual(cluster.compression, CompressionType.ZLIB)
                self.assertTrue(cluster.dirty)

    def test_get_blob_size_modified(self):
        """
        Test L{pyzim.cluster.ModifiableClusterWrapper.get_blob_size} for modified blobs.
        """
        with self.open_zts_small_dir() as zimdir:
            with zimdir.open(mode="u") as zim:
                cluster = zim.get_cluster_by_index(0)
                # as part of this test, we also delete another blob
                # so that we can test index adjustment
                cluster.remove_blob(2)
                org_size = cluster.get_blob_size(3)
                self.assertEqual(len(cluster.read_blob(3)), org_size)
                blob_source = InMemoryBlobSource("hello")
                cluster.set_blob(3, blob_source)
                self.assertEqual(cluster.get_blob_size(3), 5)

    def test_iter_blob_offsets_modified(self):
        """
        Test L{pyzim.cluster.ModifiableClusterWrapper.iter_blob_offsets} for modifed blobs.
        """
        with self.open_zts_small_dir() as zimdir:
            with zimdir.open(mode="u") as zim:
                cluster = zim.get_cluster_by_index(0)
                org_offsets = list(cluster.iter_blob_offsets())
                # as part of this test, we also delete another blob
                # so that we can test index adjustment
                removed_size = cluster.get_blob_size(2)
                cluster.remove_blob(2)
                # as a result, the offsets should be reduced
                expected_offsets = [e - 4 for e in org_offsets]  # 4 for pointer size
                expected_offsets = expected_offsets[:2] + [e - removed_size for e in expected_offsets[3:]]  # for the deleted blob
                self.assertEqual(list(cluster.iter_blob_offsets()), expected_offsets)
                # add a different blob
                old_size = cluster.get_blob_size(3)
                new_blob = InMemoryBlobSource(b"hello")
                cluster.set_blob(3, new_blob)
                new_offsets = list(cluster.iter_blob_offsets())
                # again, adjust offsets
                # - offsets 0..3 should remain the same
                # - offset 4 is offset 3 + new size
                # - offsets 5+ are updated by size difference
                new_expected_offsets = expected_offsets[:4] + [expected_offsets[3] + 5] + [e + (5 - old_size) for e in expected_offsets[5:]]
                self.assertEqual(new_expected_offsets, new_offsets)

    def test_read_blob_modified(self):
        """
        Test L{pyzim.cluster.ModifiableClusterWrapper.read_blob} for modified blobs.
        """
        with self.open_zts_small_dir() as zimdir:
            with zimdir.open(mode="u") as zim:
                cluster = zim.get_cluster_by_index(0)
                org_data = self.get_data_in_cluster(cluster)
                # as part of this test, we also delete another blob
                # so that we can test index adjustment
                cluster.remove_blob(2)
                del org_data[2]
                # read an existing blob
                data = cluster.read_blob(4)
                self.assertEqual(data, org_data[4])
                # modify a blob
                s = b"Some nice blob data"
                new_blob = InMemoryBlobSource(s)
                cluster.set_blob(5, new_blob)
                data = cluster.read_blob(5)
                self.assertEqual(data, s)

    def test_iter_read_blob_modified(self):
        """
        Test L{pyzim.cluster.ModifiableClusterWrapper.iter_read_blob} for modified blobs.
        """
        with self.open_zts_small_dir() as zimdir:
            with zimdir.open(mode="u") as zim:
                cluster = zim.get_cluster_by_index(0)
                org_data = self.get_data_in_cluster(cluster)
                # as part of this test, we also delete another blob
                # so that we can test index adjustment
                cluster.remove_blob(2)
                del org_data[2]
                # read an existing blob
                data = b""
                for chunk in cluster.iter_read_blob(4, buffersize=2):
                    data += chunk
                self.assertEqual(data, org_data[4])
                # modify a blob
                s = b"Some nice blob data"
                new_blob = InMemoryBlobSource(s)
                cluster.set_blob(5, new_blob)
                data = b""
                for chunk in cluster.iter_read_blob(5, buffersize=2):
                    data += chunk
                self.assertEqual(data, s)

    def test_get_content_size_modified(self):
        """
        Test L{pyzim.cluster.ModifiableClusterWrapper.get_content_size} for modified blobs.
        """
        with self.open_zts_small_dir() as zimdir:
            with zimdir.open(mode="u") as zim:
                cluster = zim.get_cluster_by_index(0)
                org_content_size = cluster.get_content_size()
                # delete a blob
                removed_size = cluster.get_blob_size(2)
                cluster.remove_blob(2)
                self.assertEqual(cluster.get_content_size(), org_content_size - removed_size)
                # overwrite a blob
                prev_size = cluster.get_blob_size(5)
                s = b"some data"
                new_size = len(s)
                new_blob = InMemoryBlobSource(s)
                cluster.set_blob(5, new_blob)
                # verify size
                new_content_size = cluster.get_content_size()
                expected_content_size = org_content_size - removed_size - prev_size + new_size
                self.assertEqual(new_content_size, expected_content_size)

    def test_reset_base(self):
        """
        Test L{pyzim.cluster.ModifiableClusterWrapper.reset}.
        """
        with self.open_zts_small_dir() as zimdir:
            with zimdir.open(policy=self.get_policy()) as zim:
                cluster = zim.get_cluster_by_index(0)
                cluster.read_blob(0)
                self.assertIsNotNone(cluster.compression)
                self.assertIsNotNone(cluster.is_extended)
                cluster.reset()
                self.assertIsNone(cluster.compression)
                self.assertIsNone(cluster.is_extended)
                self.assertFalse(cluster.did_read_infobyte)


class ClusterBehaviorEquivalenceTests(unittest.TestCase, TestBase):
    """
    Tests to ensure that cluster class behavior matches.
    """
    cluster_classes = [
        Cluster,
        OffsetRememberingCluster,
        InMemoryCluster,
        ModifiableClusterWrapperHelper,
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


class EmptyClusterTests(unittest.TestCase, TestBase):
    """
    Tests for L{pyzim.cluster.EmptyCluster}.

    This is not a subclass of L{ClusterTests} because we can not use an
    empty cluster for the various tests performed there.
    """
    def test_empty_cluster(self):
        """
        The various tests for the emtpty cluster.

        These tests are individually too small to justify individual tests
        """
        cluster = EmptyCluster()

        # get_blob_size()
        # an empty cluster has not blobs at all
        with self.assertRaises(exceptions.BlobNotFound):
            cluster.get_blob_size(0)

        # get_content_size
        # content size should be 0 if cluster is empty
        self.assertEqual(cluster.get_content_size(), 0)

        # get_total_compressed_size
        # 1 for the infobyte and 4 for the offset
        self.assertEqual(cluster.get_total_compressed_size(), 5)

        # get_total_decompressed_size
        # clusters default to NONE compression, so their decompressed
        # size equals the offset size
        self.assertEqual(cluster.get_total_decompressed_size(), 4)

        # get_total_offset_size
        # a cluster always has at least one offset, even if empty
        self.assertEqual(cluster.get_total_offset_size(), 4)

        # get_number_of_blobs
        # should obviously be 0
        self.assertEqual(cluster.get_number_of_blobs(), 0)

        # get_number_of_offsets
        # again, at least 1
        self.assertEqual(cluster.get_number_of_offsets(), 1)

        # get_offset
        # there is only one offset, pointing to the end of said offset
        self.assertEqual(cluster.get_offset(0), 4)
        with self.assertRaises(IndexError):
            cluster.get_offset(1)

        # iter_blob_offset
        self.assertEqual(len(list(cluster.iter_blob_offsets())), 1)
        self.assertEqual(len(list(cluster.iter_blob_offsets(blob_numbers=(0, 1, 2)))), 1)
        self.assertEqual(len(list(cluster.iter_blob_offsets(blob_numbers=(1, 2, 3)))), 0)

        # iter_read_blob
        with self.assertRaises(exceptions.BlobNotFound):
            for chunk in cluster.iter_read_blob(0):
                pass

        # read_blob
        with self.assertRaises(exceptions.BlobNotFound):
            cluster.read_blob(0)
