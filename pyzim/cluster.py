"""
Implementation of clusters.
"""
import struct
from contextlib import contextmanager

from . import constants
from .bindable import BindableMixIn
from .compression import CompressionType, CompressionRegistry, DecompressingReader
from .exceptions import BindRequired, UnsupportedCompressionType


class Cluster(BindableMixIn):
    """
    Implementation of a cluster in a ZIM file.

    A cluster contains the blobs (=content) of content entries. As these
    are compressed together, it allows for higher compression rates.

    A cluster can be extended, which means that it allows to be larger
    than 4 GiB, but will have a larger overhead.

    @ivar offset: absolute offset of the cluster
    @type offset: L{int} or L{None}
    @ivar compression: compression to use, None when unknown
    @type compression: L{pyzim.compression.CompressionType} or L{None}
    @ivar is_extended: whether this cluster is extended, None if not set
    @type is_extended: L{bool} or L{None}
    """
    def __init__(self, zim=None, offset=None):
        """
        The default constructor.

        @param zim: if specified, bind this ZIM immediately.
        @type zim: L{pyzim.archive.Zim}
        @param offset: absolute offset of the cluster
        @type offset: L{int} or L{None}
        @raises ValueError: if offset was specified but zim was not specified.
        """
        if (offset is not None and zim is None):
            raise ValueError("Offset specified but no ZIM file supplied!")
        assert offset is None or (isinstance(offset, int) and offset >= 0)

        BindableMixIn.__init__(self, zim)
        self.offset = offset
        self.compression = None
        self.is_extended = None

        # attributes for decompressor recycling
        self._decompressing_reader = None

    def _seek_if_needed(self, f, offset):
        """
        Seek to the specified position (relative to the cluster start)
        in the file only if it is needed.

        Needs to be bound.

        @param f: file to seek
        @type f: file-like object
        @param offset: offset to seek, relative to the start of the cluster
        @type offset: L{int}
        """
        assert isinstance(offset, int) and offset >= 0
        assert self.zim is not None
        abs_pos = self.offset + offset
        cur_pos = f.tell()
        if cur_pos != abs_pos:
            f.seek(abs_pos)

    @property
    def _pointer_format(self):
        """
        The pointer format.

        @return: struct format for pointers for this cluster
        @rtype: L{str}
        """
        if self.is_extended is None:
            raise RuntimeError("._pointer_format accessed before extension has been specified!")
        if self.is_extended:
            basetype = "Q"
        else:
            basetype = "I"
        return constants.ENDIAN + basetype

    def read_infobyte(self):
        """
        Read the cluster information byte, returning it.

        @return: the byte containing cluster information
        @rtype: L{bytes}
        @raises pyzim.exceptions.BindRequired: if cluster is unbound
        """
        if not self.bound:
            raise BindRequired("Reading the infobyte of a cluster required it to be bound first!")
        with self.zim.acquire_file() as fin:
            self._seek_if_needed(fin, 0)
            infobyte = fin.read(1)
            if not infobyte:
                raise IOError("Unable to read infobyte: empty read!")
        return infobyte

    def parse_infobyte(self, infobyte):
        """
        Parse the cluster information byte, setting the attributes of this
        cluster as necessary.

        @param infobyte: the cluster information byte
        @type infobyte: L{bytes} of length 1
        @raises pyzim.exceptions.UnsupportedCompressionType: if the compression type is unknown.
        """
        assert isinstance(infobyte, bytes) and len(infobyte) == 1
        as_int = ord(infobyte)
        lower_4_bits = as_int & 0b1111
        higher_4_bits = as_int >> 4
        try:
            self.compression = CompressionType(lower_4_bits)
        except ValueError:
            raise UnsupportedCompressionType("Compression {} is unknown.".format(lower_4_bits))
        self.is_extended = higher_4_bits & 1 == 1

    @property
    def did_read_infobyte(self):
        """
        True if the infobyte was already read and parsed.

        @return: True if the infobyte was already read and parsed.
        @rtype: L{bool}
        """
        return not (self.compression is None or self.is_extended is None)

    def read_infobyte_if_needed(self):
        """
        Read and parse the infobyte if this has not yet happened.
        """
        if not self.did_read_infobyte:
            infobyte = self.read_infobyte()
            self.parse_infobyte(infobyte)

    def generate_infobyte(self):
        """
        Generate the infobyte for this cluster.

        @return: the generated infobyte
        @rtype: L{bytes} of length 1
        """
        higher_4_bits = int(self.is_extended)
        lower_4_bits = self.compression.value
        value = (higher_4_bits << 4) + lower_4_bits
        return chr(value).encode("ASCII")

    def _get_decompressor(self):
        """
        Return a decompressor suitable to decompress this cluster.

        @return: a decompressor suitable to decompress this cluster
        @rtype: a decompressor-like object. See L{pyzim.compression.BaseCompressionInterface} for more info.
        """
        decompressor = CompressionRegistry.get(self.compression).get_decompressor(
            options=self.zim.policy.compression_options,
        )
        return decompressor

    @contextmanager
    def _get_decompressing_reader(self, offset=0):
        """
        Return a decompressing reader that can be sued to decompress the content.

        If offset is specified, the decompressor will have read to that offset.
        This may reuse the decompressor, depending on the implementation and the offset.

        @param offset: offset, relative to the start of the compressed data (cluster start + 1)
        @type offset: L{int}
        @raises pyzim.exceptions.BindRequired: if cluster is unbound
        """
        assert isinstance(offset, int) and offset >= 0

        if not self.bound:
            raise BindRequired("Decompressing the content of a cluster requires it to be bound to a ZIM!")
        with self.zim.acquire_file() as fin:
            reader = None
            if self._decompressing_reader is not None:
                # a decompressing reader already exists, we may be able to reuse it
                if offset >= self._decompressing_reader.read_decompressed:
                    self._decompressing_reader.reseek(base_offset=self.offset + 1)
                    # to_skip = (offset - 1) - self._decompressing_reader.read_decompressed
                    # self._decompressing_reader.skip(to_skip)
                    self._decompressing_reader.skip_to(offset)
                    reader = self._decompressing_reader
            if reader is None:
                # could not recycle reader
                fin.seek(self.offset + 1)
                decompressor = self._get_decompressor()
                self._decompressing_reader = reader = DecompressingReader(fin, decompressor)
                reader.skip_to(offset)
            yield reader

    def iter_blob_offsets(self, blob_numbers=None):
        """
        Read the blob offsets, yielding them as an iterator.

        The order of blob_numbers does not matter, all offsets are always
        yielded in regular order (offfset 1, offset 2, ...).

        @param blob_numbers: if specified, load only these offsets
        @type blob_numbers: L{None} or L{list} of L{int}
        @yields: the offset of each blob in the decompressed body, relative to cluster start
        @ytype: L{int}
        @raises pyzim.exceptions.BindRequired: if cluster is unbound
        """
        assert isinstance(blob_numbers, (list, tuple)) or (blob_numbers is None)
        if not self.bound:
            raise BindRequired("Accessing blob offsets requires the cluster to be bound first!")
        self.read_infobyte_if_needed()
        pointer_size = struct.calcsize(self._pointer_format)
        if blob_numbers is not None:
            if not blob_numbers:
                # empty, return no offsets
                return []
            last_offset_to_read = max(blob_numbers)
        else:
            last_offset_to_read = None
        first_offset = None
        n_bytes_read = 0
        offset_number = 0

        with self._get_decompressing_reader() as reader:
            # Loop conditions:
            # - always read first offset, as it hints at the number of offsets
            # - never read further than the value of the first offset
            # - exit loop early if all blob that should be read have been read
            while (
                ((first_offset is None) or (n_bytes_read + 1 <= first_offset))
                and ((last_offset_to_read is None) or (offset_number <= last_offset_to_read))
            ):
                offset_raw = reader.read_n(pointer_size)
                n_bytes_read += len(offset_raw)
                offset = struct.unpack(self._pointer_format, offset_raw)[0]

                if first_offset is None:
                    first_offset = offset

                if blob_numbers is not None:
                    if offset_number in blob_numbers:
                        yield offset
                else:
                    yield offset

                offset_number += 1

    def get_offset(self, i):
        """
        Return the offset with the specified index.

        @param i: index of blob to get offset for
        @type i: L{int}
        @raises IndexError: if i < 0 or i >= len(offsets)
        @raises pyzim.exceptions.BindRequired: if cluster is unbound
        """
        assert isinstance(i, int)
        if not self.bound:
            raise BindRequired("Accessing blob offsets requires the cluster to be bound first!")
        if i < 0:
            raise IndexError("Offset {} is negative, no offsets with negative indexes can exist.".format(i))
        for offset in self.iter_blob_offsets(blob_numbers=(i, )):
            # this loop body should only be executed once for the specified  index
            return offset
        # if we've reached this part of the code, then i was larger than
        # the number of indexes
        raise IndexError("Offset/Blob index {} not found in cluster!".format(i))

    def get_number_of_offsets(self):
        """
        Return the number of offsets in this cluster.

        This value differs from the number of blobs in the cluster.

        @return: the number of offsets in this cluster.
        @rtype: L{int}
        @raises pyzim.exceptions.BindRequired: if cluster is unbound
        """
        if not self.bound:
            raise BindRequired("Accessing blob offsets requires the cluster to be bound first!")
        # we can calculate the number of offsets directly from the first offset
        offset_1 = self.get_offset(0)
        pointer_size = struct.calcsize(self._pointer_format)
        return offset_1 // pointer_size

    def get_number_of_blobs(self):
        """
        Return the number of blobs in this cluster.

        This value differs from the number of offsets in the cluster.

        @return: the number of blobs in this cluster
        @rtype: L{int}
        @raises pyzim.exceptions.BindRequired: if cluster is unbound
        """
        return self.get_number_of_offsets() - 1

    def get_content_size(self):
        """
        Return the content size of this cluster.

        This is the uncompressed size of the content of this cluster,
        not including the offsets and infobyte.

        @return: the size of the content of this cluster
        @rtype: L{int}
        @raises pyzim.exceptions.BindRequired: if cluster is unbound
        """
        if not self.bound:
            raise BindRequired("Accessing blob sizes requires the cluster to be bound first!")

        # TODO: avoid reading offset 1 multiple times
        offset_1 = self.get_offset(0)
        offset_l = self.get_offset(self.get_number_of_offsets() - 1)
        return offset_l - offset_1

    def get_total_decompressed_size(self):
        """
        Return the total decompressed size of this cluster.

        This is the uncompressed size of the content of this cluster,
        including the offsets but not the infobyte.

        @return: the size of the content of this cluster including offsets
        @rtype: L{int}
        @raises pyzim.exceptions.BindRequired: if cluster is unbound
        """
        if not self.bound:
            raise BindRequired("Accessing blob sizes requires the cluster to be bound first!")

        # TODO: avoid reading offset 1 multiple times
        offset_l = self.get_offset(self.get_number_of_offsets() - 1)
        return offset_l

    def get_total_offset_size(self):
        """
        Return the total size of the offsets.

        @return: the total size of the offsets in bytes
        @rtype: L{int}
        """
        return self.get_number_of_offsets() * struct.calcsize(self._pointer_format)

    '''
    def get_total_compressed_size(self):
        """
        Return the total compressed size of the cluster.

        This includes the entirety of the cluster, including the infobyte.

        @return: the size of this cluster
        @rtype: L{int}
        @raises pyzim.exceptions.BindRequired: if cluster is unbound
        """
        if not self.bound:
            raise BindRequired("Accessing cluster size requires the cluster to be bound first!")
        # clusters unfortunately do not know their own size
        # instead, the end is determined by the start of the next cluster.
        own_i = self.zim.get_cluster_index_by_offset(self.offset)
        next_i = own_i + 1
        next_cluster = self.zim.get_cluster_by_index(next_i)
        return next_cluster.offset - self.offset
    '''

    def get_blob_size(self, i):
        """
        Get the size of a blob.

        @param i: index of blob to get size for
        @type i: L{int}
        @return: the size of the uncompressed blob
        @rtype: L{int}
        @raises pyzim.exceptions.BindRequired: if cluster is unbound
        """
        assert isinstance(i, int) and i >= 0
        if not self.bound:
            raise BindRequired("Accessing blob sizes requires the cluster to be bound first!")

        offsets = tuple(self.iter_blob_offsets(blob_numbers=(i, i+1)))
        return offsets[1] - offsets[0]

    def read_blob(self, i):
        """
        Read the entirety of the specified blob and return the content.

        @param i: index of blob to read
        @type i: L{int}
        @return: the content of the blob
        @rtype: L{bytes}
        @raises pyzim.exceptions.BindRequired: if cluster is unbound
        """
        assert isinstance(i, int) and i >= 0
        if not self.bound:
            raise BindRequired("Accessing blob contents requires the cluster to be bound first!")

        offsets = tuple(self.iter_blob_offsets(blob_numbers=(i, i+1)))
        size = offsets[1] - offsets[0]  # not using get_blob_size() as we need the offset
        with self._get_decompressing_reader(offset=offsets[0]) as reader:
            return reader.read_n(size)

    def iter_read_blob(self, i, buffersize=4096):
        """
        Iteratively read the specified blob.

        @param i: index of blob to read
        @type i: L{int}
        @param buffersize: number of bytes to read at once
        @type buffersize: L{int}
        @yields: chunks of the blob content
        @ytype: L{bytes}
        @raises pyzim.exceptions.BindRequired: if cluster is unbound
        """
        assert isinstance(i, int) and i >= 0
        assert isinstance(buffersize, int) and buffersize > 0
        if not self.bound:
            raise BindRequired("Accessing blob contents requires the cluster to be bound first!")

        offsets = tuple(self.iter_blob_offsets(blob_numbers=(i, i+1)))
        size = offsets[1] - offsets[0]  # not using get_blob_size() as we need the offset
        with self._get_decompressing_reader(offset=offsets[0]) as reader:
            yield from reader.iter_read(n=size, buffersize=buffersize)


class OffsetRememberingCluster(Cluster):
    """
    A variation of L{Cluster} that reads offsets only once, storing
    them in memory.
    """
    def __init__(self, zim=None, offset=None):
        Cluster.__init__(self, zim=zim, offset=offset)
        self._offsets = None

    def _read_offsets_if_needed(self):
        """
        Read the offsets if they have not yet been read.

        @raises pyzim.exceptions.BindRequired: if cluster is unbound
        """
        if self._offsets is None:
            self._offsets = [o for o in Cluster.iter_blob_offsets(self)]

    def iter_blob_offsets(self, blob_numbers=None):
        assert isinstance(blob_numbers, (list, tuple)) or blob_numbers is None
        self._read_offsets_if_needed()
        for i, offset in enumerate(self._offsets):
            if blob_numbers is None:
                yield offset
            elif i in blob_numbers:
                yield offset


class InMemoryCluster(OffsetRememberingCluster):
    """
    A variation of L{Cluster} that decompresses only once, storing all
    data in RAM.
    """
    def __init__(self, zim=None, offset=None):
        OffsetRememberingCluster.__init__(self, zim=zim, offset=offset)
        self._data = None

    def _read_if_needed(self):
        """
        Read (and decompress) all data if this has not yet been done.

        @raises pyzim.exceptions.BindRequired: if cluster is unbound
        """
        if not self.bound:
            raise BindRequired("Reading data from cluster requires it to be bound!")
        if self._data is None:
            self.read_infobyte_if_needed()
            # preferably, we would only have the minimal amount of reads
            # unfortunately, we can only know the size of a cluster by
            # reading the offsets
            # own_size = self.get_total_compressed_size()
            # with self.zim.acquire_file() as f:
            #     raw_data = read_n_bytes(f, own_size, raise_on_incomplete=True)
            # decompressor = self._get_decompressor()
            # self._data = decompressor.decompress(raw_data)

            OffsetRememberingCluster._read_offsets_if_needed(self)
            data_length = self.get_content_size()
            with self._get_decompressing_reader(offset=self.get_offset(0)) as reader:
                self._data = reader.read_n(data_length)

    def read_blob(self, i):
        assert isinstance(i, int) and i >= 0
        if not self.bound:
            raise BindRequired("Accessing blob contents requires the cluster to be bound first!")

        self._read_if_needed()
        offsets = tuple(self.iter_blob_offsets(blob_numbers=(i, i+1)))
        correction = self.get_total_offset_size()
        return self._data[offsets[0]-correction:offsets[1]-correction]

    def iter_read_blob(self, i, buffersize=4096):
        assert isinstance(i, int) and i >= 0
        assert isinstance(buffersize, int) and buffersize > 0
        if not self.bound:
            raise BindRequired("Accessing blob contents requires the cluster to be bound first!")

        self._read_if_needed()

        correction = self.get_total_offset_size()
        start, end = tuple(self.iter_blob_offsets(blob_numbers=(i, i+1)))
        start, end = start - correction, end - correction

        cur_pos = start
        while cur_pos < end:
            cur_end = min(cur_pos + buffersize, end)
            yield self._data[cur_pos:cur_end]
            cur_pos = cur_end


if __name__ == "__main__":  # pragma: no cover
    # test script
    import argparse

    from .archive import Zim

    parser = argparse.ArgumentParser(description="Print info about a cluster in a ZIM file")
    parser.add_argument("path", help="path to file to read")
    parser.add_argument("cluster", type=int, nargs="?", default=0, help="Cluster index (default: 0)")
    parser.add_argument("--with-content", action="store_true", dest="withcontent", help="Also print blob contents")
    ns = parser.parse_args()

    with Zim.open(ns.path, mode="r") as zim:
        cluster = zim.get_cluster_by_index(ns.cluster)
        cluster.read_infobyte_if_needed()
        print("Cluster: {}".format(ns.cluster))
        print("Offset: {}".format(cluster.offset))
        print("Extended: {}".format(cluster.is_extended))
        print("Compression: {!r}".format(cluster.compression))
        print("Number of offsets: {}".format(cluster.get_number_of_offsets()))
        print("Number of blobs: {}".format(cluster.get_number_of_blobs()))
        print("First offset: {}".format(cluster.get_offset(0)))
        print("Last offset: {}".format(cluster.get_offset(cluster.get_number_of_offsets() - 1)))
        print("Content size: {}".format(cluster.get_content_size()))
        print("Total decompressed size: {}".format(cluster.get_total_decompressed_size()))

        if ns.withcontent:
            print("====== CONTENT =====")
            for i in range(cluster.get_number_of_blobs()):
                print(cluster.read_blob(i))
                print("\n" * 3)
