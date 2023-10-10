"""
Implementation of clusters.
"""
import struct
from contextlib import contextmanager

from . import constants
from .bindable import BindableMixIn
from .blob import BaseBlobSource, EmptyBlobSource
from .compression import CompressionType, CompressionRegistry, DecompressingReader
from .exceptions import BindRequired, UnsupportedCompressionType, BlobNotFound
from .modifiable import ModifiableMixIn


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

    def reset(self):
        """
        Reset all internal state except the cluster offset, causing said
        offset to be read again the next time it is required.
        """
        self.compression = None
        self.is_extended = None

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
        @raises RuntimeError: if accessed before extension has been specified
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

    def _get_compressor(self):
        """
        Return a compressor suitable to compress this cluster.

        @return: a compressor suitable to compress this cluster.
        @rtype: a compressor-like object. See L{pyzim.compression.BaseCompressionInterface} for more info.
        """
        self.read_infobyte_if_needed()
        compressor = CompressionRegistry.get(self.compression).get_compressor(
            options=self.zim.policy.compression_options,
        )
        return compressor

    def _get_decompressor(self):
        """
        Return a decompressor suitable to decompress this cluster.

        @return: a decompressor suitable to decompress this cluster
        @rtype: a decompressor-like object. See L{pyzim.compression.BaseCompressionInterface} for more info.
        """
        self.read_infobyte_if_needed()
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
        # in order to avoid a situation where multiple methods recursively
        # attempt to acquire the file lock, we read the infobyte needed
        # for the compression already here if it is required
        self.read_infobyte_if_needed()
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

    def get_total_compressed_size(self):
        """
        Return the total compressed size of the cluster.

        This includes the entirety of the cluster, including the infobyte.

        NOTE: this method is horribly inefficient, as it requires
        decompressing the entire cluster

        @return: the size of this cluster
        @rtype: L{int}
        @raises pyzim.exceptions.BindRequired: if cluster is unbound
        """
        if not self.bound:
            raise BindRequired("Accessing cluster size requires the cluster to be bound first!")
        # clusters unfortunately do not know their own size
        # instead, we have to read and decompress it
        self.read_infobyte_if_needed()
        if self.compression == CompressionType.NONE:
            # no compression
            # this means that we can just use the raw size
            # also, eof does not work with the passtrough compressor
            return self.get_total_decompressed_size() + 1
        else:
            # we meed to decompress the cluster
            with self._get_decompressing_reader() as reader:
                # read until the end
                for chunk in reader.iter_read():
                    pass
                return reader.total_compressed_size + 1

    def get_blob_size(self, i):
        """
        Get the size of a blob.

        @param i: index of blob to get size for
        @type i: L{int}
        @return: the size of the uncompressed blob
        @rtype: L{int}
        @raises pyzim.exceptions.BindRequired: if cluster is unbound
        @raises pyzim.exceptions.BlobNotFound: if the specified blob does not exists
        """
        assert isinstance(i, int) and i >= 0
        if not self.bound:
            raise BindRequired("Accessing blob sizes requires the cluster to be bound first!")

        offsets = tuple(self.iter_blob_offsets(blob_numbers=(i, i+1)))
        if len(offsets) < 2:
            raise BlobNotFound("Blob with index {} not found!".format(i))
        return offsets[1] - offsets[0]

    def read_blob(self, i):
        """
        Read the entirety of the specified blob and return the content.

        @param i: index of blob to read
        @type i: L{int}
        @return: the content of the blob
        @rtype: L{bytes}
        @raises pyzim.exceptions.BindRequired: if cluster is unbound
        @raises pyzim.exceptions.BlobNotFound: if the blob index is out of range
        """
        assert isinstance(i, int) and i >= 0
        if not self.bound:
            raise BindRequired("Accessing blob contents requires the cluster to be bound first!")

        offsets = tuple(self.iter_blob_offsets(blob_numbers=(i, i+1)))
        if len(offsets) < 2:
            # blob offsets not found, blob does not exists
            raise BlobNotFound("No blob with index {}!".format(i))
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
        @raises pyzim.exceptions.BlobNotFound: if the blob index is out of range
        """
        assert isinstance(i, int) and i >= 0
        assert isinstance(buffersize, int) and buffersize > 0
        if not self.bound:
            raise BindRequired("Accessing blob contents requires the cluster to be bound first!")

        offsets = tuple(self.iter_blob_offsets(blob_numbers=(i, i+1)))
        if len(offsets) < 2:
            # blob offsets not found, blob does not exists
            raise BlobNotFound("No blob with index {}!".format(i))
        size = offsets[1] - offsets[0]  # not using get_blob_size() as we need the offset
        with self._get_decompressing_reader(offset=offsets[0]) as reader:
            yield from reader.iter_read(n=size, buffersize=buffersize)


class OffsetRememberingCluster(Cluster):
    """
    A variation of L{Cluster} that reads offsets only once, storing
    them in memory.

    @ivar _offsets: the offsets in this cluster
    @type _offsets: L{list} of L{int}
    """
    def __init__(self, zim=None, offset=None):
        Cluster.__init__(self, zim=zim, offset=offset)
        self._offsets = None

    def reset(self):
        self._offsets = None
        Cluster.reset(self)

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

    def reset(self):
        self._data = None
        OffsetRememberingCluster.reset(self)

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
        if len(offsets) < 2:
            # blob offsets not found, blob does not exists
            raise BlobNotFound("No blob with index {}!".format(i))
        correction = self.get_total_offset_size()
        return self._data[offsets[0]-correction:offsets[1]-correction]

    def iter_read_blob(self, i, buffersize=4096):
        assert isinstance(i, int) and i >= 0
        assert isinstance(buffersize, int) and buffersize > 0
        if not self.bound:
            raise BindRequired("Accessing blob contents requires the cluster to be bound first!")

        self._read_if_needed()

        correction = self.get_total_offset_size()
        offsets = tuple(self.iter_blob_offsets(blob_numbers=(i, i+1)))
        if len(offsets) < 2:
            # blob offsets not found, blob does not exists
            raise BlobNotFound("No blob with index {}!".format(i))
        start, end = offsets
        start, end = start - correction, end - correction

        cur_pos = start
        while cur_pos < end:
            cur_end = min(cur_pos + buffersize, end)
            yield self._data[cur_pos:cur_end]
            cur_pos = cur_end


class EmptyCluster(Cluster):
    """
    A special type of L{pyzim.cluster.Cluster} that is always empty.

    This is used for creating new clusters.
    """
    def __init__(self, zim=None):
        """
        The default constructor.
        """
        Cluster.__init__(self, zim=zim)
        self.compression = CompressionType.NONE  # makes some size calculations easier
        self.is_extended = False

    def reset(self):
        self.compression = CompressionType.NONE
        self.is_extended = False

    def get_blob_size(self, i):
        assert isinstance(i, int) and i >= 0
        raise BlobNotFound("Empty clusters are always empty!")

    def get_content_size(self):
        return 0

    def get_total_compressed_size(self):
        return 5  # for the infobyte + offset

    def get_total_decompressed_size(self):
        return 4  # for the offset

    def get_total_offset_size(self):
        return 4  # a single offset

    def get_number_of_blobs(self):
        return 0

    def get_number_of_offsets(self):
        # a cluster always has at least one offset
        return 1

    def get_offset(self, i):
        assert isinstance(i, int) and i >= 0
        if i == 0:
            # the 0 offset points to the end of the offsets
            return struct.calcsize(self._pointer_format)
        raise IndexError("No such offset in empty cluster: {}".format(i))

    def iter_blob_offsets(self, blob_numbers=None):
        if (blob_numbers is None) or (0 in blob_numbers):
            # the first offset
            yield 4

    def iter_read_blob(self, i, buffersize=4096):
        assert isinstance(i, int) and i >= 0
        assert isinstance(buffersize, int) and buffersize >= 0
        raise BlobNotFound("Empty clusters are always empty!")

    def read_blob(self, i):
        assert isinstance(i, int) and i >= 0
        raise BlobNotFound("Empty clusters are always empty!")

    def read_infobyte(self):
        # what should we do here?
        raise NotImplementedError("Reading the infobyte is not yet implemented for empty clusters")

    def read_infobyte_if_needed(self):
        # do nothing, as we do not have to read
        return


class ModifiableClusterWrapper(Cluster, ModifiableMixIn):
    """
    A special type of cluster that wraps another cluster and adds methods for modifying the cluster.

    This type of cluster is used when creating a new ZIM file or when
    modifying an existing one.

    @cvar DEFAULT_BLOB_READ: how many bytes to read from a blob at once
    @type DEFAULT_BLOB_READ: L{int}

    @ivar _cluster: the wrapped cluster. Any binding will be inherited.
    @type _cluster: L{Cluster}
    @ivar _force_extension: if not None, use this value for L{ModifiableClusterWrapper.is_extended}
    @type _force_extension: L{None} or L{bool}
    @ivar _force_compression: if not None, use this value for L{Cluster.compression}
    @type _force_compression: L{None} or L{pyzim.compression.CompressionType}
    @ivar _blobs: a dict mapping blob number to the new/modified blobs
    @type _blobs: L{dict} mapping L{int} to L{pyzim.blob.BaseBlobSource}
    @ivar _added_blobs: a list of indexes of newly added blobs
    @type _added_blobs: L{list} of L{int}
    @ivar _removed_blobs: a sorted list of removed blob numbers
    @type _removed_blobs: L{list} of L{int}
    """

    DEFAULT_BLOB_READ = 8192

    def __init__(self, cluster):
        """
        The default constructor.

        @param cluster: cluster to wrap
        @type cluster: L{Cluster}
        """
        # we need to set _cluster first, so that the various methods
        # called by the constructors can access it
        assert isinstance(cluster, Cluster)
        self._cluster = cluster
        # proceed with initialization
        Cluster.__init__(self, zim=self._cluster.zim, offset=self._cluster.offset)
        ModifiableMixIn.__init__(self)

        self._force_extension = None
        self._force_compression = None
        self._blobs = {}
        self._added_blobs = []
        self._removed_blobs = []
        if self.bound:
            # we can only find the size if the cluster is bound
            self.after_flush_or_read()

    def reset(self):
        # reset wrapped cluster and all modifications
        if isinstance(self._cluster, EmptyCluster):
            # special case: the empty cluster is intended as a helper
            # for the wrapper. There is no situation in which we should
            # reset this cluster when wrapping an empty cluster
            # this may be slightly unexpected behavior for users, but
            # do not reset this wrapper in this case.
            return
        self._cluster.reset()
        self._force_extension = None
        self._force_compression = None
        self._blobs = {}
        self._added_blobs = []
        self._removed_blobs = []
        Cluster.reset(self)

    def after_flush_or_read(self):
        self.reset()
        ModifiableMixIn.after_flush_or_read(self)

    @property
    def _has_modifications(self):
        """
        Return True if this wrapper has any modifications registered.

        @return: if any modifications are present
        @rtype: L{bool}
        """
        return bool(self._blobs or self._added_blobs or self._removed_blobs)

    def _adjust_index(self, i):
        """
        Adjust an index to work with deletions.

        Basically, if an index is deleted, all further indexes get
        reduced by one. As deletions only happen virtually in the
        wrapper and the wrapped cluster still use the original index
        system, such indexes need to be adjusted.

        Speaking from experience, it's rater easy to get confused on
        when to use the adjusted index. The simplified answer is: if it
        is passed to the wrapped cluster, use the adjusted index.
        Otherwise, use the raw index.

        @param i: index to adjust
        @type i: L{int}
        @return: the adjusted index
        @rtype: L{int}
        """
        for removed_index in self._removed_blobs:
            if i >= removed_index:
                # index affected by deletion, increment by one
                # as the list is sorted, this *should* be called once
                # per deletion even with the increment
                i += 1
        return i

    # the following method turned out to not be needed
    # it is also very untested and has thus been commented out
    # still, it was quite some work to write and perhaps we need it in
    # the future, so I am leaving this code here. Just be warned that it
    # is untested.
    '''
    def _adjust_offset_w2a(self, offset):
        """
        Adjust an offset from the wrapper to the actual offset in the wrapped cluster.

        This method is necessary because modifications to blobs cause
        in subsequent offsets. As the wrapped cluster still uses the
        non-modified offsets, we need to adjust them.

        @param offset: the offset as the wrapper sees them
        @type offset: L{int}
        @return: the actual offset in the wrapped cluster
        @rtype: L{int}
        @raises ValueError: if the offset is outside of the original blob
        """
        assert isinstance(offset, int) and offset >= 0
        # each offset is corrected by a correction factor
        # this correction factor is as following
        # - difference of length of modified blobs
        # + removed blobs length
        correction = 0
        wrapped_offsets = list(self._cluster.iter_blob_offsets())
        num_offsets = len(wrapped_offsets)
        for i in range(num_offsets):
            corrected_offset = offset + correction
            wrapped_offset = wrapped_offsets[i]
            has_next = i < num_offsets - 1
            if not has_next:
                # there is no offset after this one
                # this means that the offset is outside the wrapped clusters
                # offsets
                raise KeyError("Offset {} is outside of the offset range in the wrapped cluster!".format(offset))
            next_offset = wrapped_offsets[i+1]
            in_range = (wrapped_offset <= corrected_offset) and (next_offset > corrected_offset)
            if in_range:
                # the offset is for the current wrapped offset range
                return corrected_offset
            else:
                # we have not yet found the range in which the wrapped offset is
                # so we need to update the correction factor if this
                # offset range is modified
                offset_range_size = (next_offset - wrapped_offset)
                if i in self._removed_blobs:
                    # this offset range has been removed
                    # thus, the wrapped offset is reduced by this length
                    correction += offset_range_size
                elif i in self._blobs:
                    # the blob has been modified
                    # thus, reduce correction factor by number of additional bytes
                    additional_size = self._blobs[i].get_size() - offset_range_size
                    correction -= additional_size
        # if we reach here, then the for() loop never ran
        raise KeyError("Can not adjust index in empty cluster!")
    '''

    def remove_blob(self, i):
        """
        Remove the specified blob.

        NOTE: it is not recommended to use this method. Removing a blob
        also requires changing the blob numbers in entries pointing to
        blobs after the specified blob. This method does not take care
        of this. You should use L{ModifiableClusterWrapper.empty_blob}
        instead.

        @param i: index of blob to remove
        @type i: L{int}
        @raises pyzim.exceptions.NonMutable: if this cluster is set to be inmutable.
        """
        assert isinstance(i, int) and i >= 0
        self.ensure_mutable()
        i = self._adjust_index(i)
        if i not in self._added_blobs:
            # don't mark blobs as removed that were just added
            # this makes it way harder for use to keep track of the
            # number of blobs we have
            self._removed_blobs.append(i)
            self._removed_blobs.sort()
        if i in self._blobs:
            del self._blobs[i]
        if i in self._added_blobs:
            self._added_blobs.remove(i)
        # as we've removed a blob, all other indexes following this one
        # need to be adjusted
        self._blobs = {
            (blob_num - 1 if blob_num > i else blob_num): blob
            for blob_num, blob in self._blobs.items()
        }
        self.mark_dirty()

    def empty_blob(self, i):
        """
        Set the content of a blob to empty.

        This does not delete the specified blob, but reduces its size to
        0 (plus an additonal 4/8 bytes for the offset). The advantage of
        this method over L{ModifiableClusterWrapper.remove_blob} is that
        any entries linking to subsequent blobs do not need to be modified.

        @param i: index of blob to empty
        @type i: L{int}
        @raises pyzim.exceptions.NonMutable: if this cluster is set to be inmutable.
        """
        assert isinstance(i, int) and i >= 0
        self.ensure_mutable()
        # i = self._adjust_index(i)
        self.set_blob(i, EmptyBlobSource())

    def set_blob(self, i, blob_source):
        """
        Set the blob for the specified index.

        Index must be <= number of blobs. If equal, the blob will be appended.

        @param i: index of blob to set
        @type i: L{int}
        @param blob_source: source for the new blob
        @type blob_source: L{BaseBlobSource}
        @raises pyzim.exceptions.NonMutable: if this cluster is set to be inmutable.
        """
        num_blobs = self.get_number_of_blobs()
        assert isinstance(i, int) and i <= num_blobs
        assert isinstance(blob_source, BaseBlobSource)
        self.ensure_mutable()
        if i in self._removed_blobs:
            self._removed_blobs.remove(i)
        if i == num_blobs:
            # blob will be appended
            self._added_blobs.append(i)
        self._blobs[i] = blob_source
        self.mark_dirty()

    def append_blob(self, blob_source):
        """
        Append a blob to this cluster.

        @param blob_source: source for the new blob
        @type blob_source: L{BaseBlobSource}
        @return: the index (=blob number) of the new blob
        @rtype: L{int}
        @raises pyzim.exceptions.NonMutable: if this cluster is set to be inmutable.
        """
        assert isinstance(blob_source, BaseBlobSource)
        self.ensure_mutable()
        num_blobs = self.get_number_of_blobs()
        self.set_blob(num_blobs, blob_source)
        return num_blobs

    def iter_write(self):
        """
        Iteratively serialize this cluster, yielding each chunk that must be written.

        @yields: the data of this cluster as it needs to be written
        @ytype: L{bytes}
        """
        self.read_infobyte_if_needed()
        yield self.generate_infobyte()
        compressor = self._get_compressor()
        for chunk in self._iter_write_raw():
            compressed_chunk = compressor.compress(chunk)
            yield compressed_chunk
        yield compressor.flush()

    def _iter_write_raw(self):
        """
        Iteratively serialize the compressed part of this cluster, yielding each chunk that must be compressed.

        @yields: the data of this cluster as it needs to be compressed and then written
        @ytype: L{bytes}
        """
        # write offsets
        offset_s = b""
        total_offset_size = self.get_total_offset_size()
        cur_offset = total_offset_size
        for size in self._iter_blob_sizes():
            offset_s += struct.pack(self._pointer_format, cur_offset)
            cur_offset += size
        offset_s += struct.pack(self._pointer_format, cur_offset)
        yield offset_s
        # write blobs
        for i in range(self.get_number_of_blobs()):
            adjusted_i = self._adjust_index(i)
            if i in self._blobs:
                # modified blob
                blob_source = self._blobs[i]
                blob = blob_source.get_blob()
                while True:
                    data = blob.read(self.DEFAULT_BLOB_READ)
                    if not data:
                        break
                    yield data
            else:
                # original blob
                yield self._cluster.read_blob(adjusted_i)

    def _iter_blob_sizes(self):
        """
        Iterate over the blob sizes.

        This only includes blobs that are not removed.

        @yields: the blob sizes of each blob in bytes
        @ytype: L{int}
        """
        for i in range(self.get_number_of_blobs()):
            adjusted_i = self._adjust_index(i)
            if i in self._blobs:
                yield self._blobs[i].get_size()
            else:
                yield self._cluster.get_blob_size(adjusted_i)

    def flush(self):
        """
        If this cluster has been modified, write it it to the archive.

        @raises pyzim.exceptions.BindRequired: if unbound
        """
        if not self.bound:
            raise BindRequired("Cluster must be bound to allow direct flushing!")
        if self.dirty:
            self.zim.write_cluster(self)

    # ----- cluster methods -----

    @property
    def is_extended(self):
        """
        Check whether this cluster needs to be an extended cluster.

        Note that this function does not return the extension status as
        set in the cluster information byte, but calculates the required
        extension status based on the offsets. It may be forcefully set
        to a specific value.

        @return: True if this cluster needs to be an extended cluster.
        @rtype: L{bool}
        """
        # TODO: perhaps make this also the behavior of Cluster?
        if self._force_extension is None:
            # determine if cluster needs to be extended
            # we can not use get_total_decompressed_size() here as this
            # would result in an infinite recursion
            if not self._has_modifications:
                # no modifications necesssary, return value from wrapped cluster
                return self._cluster.is_extended
            self._cluster.read_infobyte_if_needed()
            num_offsets = self.get_number_of_offsets()
            min_offset_size = 4 * num_offsets
            total_size = min_offset_size
            for blob_size in self._iter_blob_sizes():
                total_size += blob_size
            if total_size >= 2 ** 32:
                # size can not fit inside 32 bit (4 byte) integers
                # extension necessary
                return True
            else:
                return False
        else:
            # extension status is force-setted
            return self._force_extension

    @is_extended.setter
    def is_extended(self, value):
        """
        Force-set the extension status of this cluster.

        Set to L{None} to disable force setting.

        @param value: new value for the extension state, L{None} to disable.
        @type value: L{bool} or L{None}
        """
        assert isinstance(value, bool) or (value is None)
        self._force_extension = value

    @property
    def offset(self):
        """
        Absolute offset of the cluster.

        @return: the absolute offset of the cluster
        @rtype: L{int} or L{None}
        """
        return self._cluster.offset

    @offset.setter
    def offset(self, value):
        """
        Set the offset.

        @param value: the new offset
        @type value: L{int} or L{None}
        """
        assert (isinstance(value, int) and value >= 0) or (value is None)
        self._cluster.offset = value

    @property
    def compression(self):
        """
        Compression to use, None when unknown.

        @return: the compression to use
        @rtype: L{pyzim.compression.CompressionType} or L{None}
        """
        if self._force_compression is not None:
            return self._force_compression
        return self._cluster.compression

    @compression.setter
    def compression(self, value):
        """
        Set the compression to use.

        @param value: value to set
        @type value: L{pyzim.compression.CompressionType} or L{int} or L{None}
        @raises TypeError: on type error
        @raises pyzim.exceptions.UnsupportedCompressionType: when value is an int not registered in L{pyzim.compression.CompressionType}
        """
        if not (isinstance(value, (CompressionType, int)) or value is None):
            raise TypeError("Compression must be a CompressionType, int or None, not {}!".format(type(value)))
        if isinstance(value, int):
            try:
                value = CompressionType(value)
            except ValueError:
                raise UnsupportedCompressionType("Compression type {} not known!".format(value))
        self._force_compression = value
        if value is not None:
            self.mark_dirty()

    def parse_infobyte(self, infobyte):
        # we need to keep track of the force-set extension status
        old_extension = self._force_extension
        # Cluster.parse_infobyte(self, infobyte)
        self._cluster.parse_infobyte(infobyte)
        self._force_extension = old_extension

    def read_infobyte(self):
        return self._cluster.read_infobyte()

    @property
    def did_read_infobyte(self):
        return self._cluster.did_read_infobyte

    def get_blob_size(self, i):
        adjusted_i = self._adjust_index(i)
        if i in self._blobs:
            # use overloaded blobs
            source = self._blobs[i]
            return source.get_size()
        else:
            # get from the wrapped cluster
            return self._cluster.get_blob_size(adjusted_i)

    def get_content_size(self):
        size = 0
        num_blobs = self.get_number_of_blobs()
        for i in range(num_blobs):
            if i in self._blobs:
                # use size of overwritten blobs
                blob_size = self._blobs[i].get_size()
                size += blob_size
            else:
                # get from original cluster
                adjusted_i = self._adjust_index(i)
                blob_size = self._cluster.get_blob_size(adjusted_i)
                size += blob_size
        return size

    def get_total_decompressed_size(self):
        return self.get_total_offset_size() + self.get_content_size()

    def get_number_of_offsets(self):
        # the number of offsets consists of:
        # +1 to indicate the end of the offsets
        # -1 for each removed blob
        # +1 for each added blob
        # +1 for each blob in wrapped cluster
        num_blobs = self._cluster.get_number_of_blobs()
        num_blobs -= len(self._removed_blobs)
        num_blobs += len(self._added_blobs)
        num_offsets = num_blobs + 1
        return num_offsets

    def get_offset(self, i):
        if i < 0:
            raise IndexError("Offset index must be at least 0, got {} instead!".format(i))
        if not self.bound:
            raise BindRequired("Accessing blob offsets requires the cluster to be bound first!")
        for offset in self.iter_blob_offsets(blob_numbers=(i, )):
            # should be executed at most once
            return offset
        raise IndexError("Offset {} not found!".format(i))

    def get_total_offset_size(self):
        if not self.bound:
            raise BindRequired("Accessing blob offsets requires the cluster to be bound first!")
        total_offset_size = struct.calcsize(self._pointer_format) * self.get_number_of_offsets()
        return total_offset_size

    def iter_blob_offsets(self, blob_numbers=None):
        assert isinstance(blob_numbers, (list, tuple)) or (blob_numbers is None)
        if not self.bound:
            raise BindRequired("Accessing blob offsets requires the cluster to be bound first!")
        start_offset = self.get_total_offset_size()
        num_offsets = self.get_number_of_offsets()
        if blob_numbers is None:
            # need to read all offsets
            last_offset = num_offsets
        elif len(blob_numbers) == 0:
            # early exit - nothing to yield
            # by setting last_offset=-1, range() will be called with end=0.
            # causing such an early exit
            # we do this because some python versions don't support return
            # inside a generator function
            last_offset = -1
        else:
            # only read until last required offset
            last_offset = max(blob_numbers)
        # approach:
        # - keep track of current offset
        # - iterate over all offset/blob indexes
        # - yield offset if it should be yieleded
        # - update current offset with size of current blob
        cur_offset = start_offset
        for i in range(last_offset + 1):
            if (blob_numbers is None) or (i in blob_numbers):
                yield cur_offset
            if i == num_offsets - 1:
                # this was the last offset
                # leave loop early and don't update the offset
                break
            adjusted_i = self._adjust_index(i)
            if i in self._blobs:
                # use overwritten blob
                size = self._blobs[i].get_size()
            else:
                # use wrapped blob
                size = self._cluster.get_blob_size(adjusted_i)
            # advance offset by blob size
            cur_offset += size

    def read_blob(self, i):
        adjusted_i = self._adjust_index(i)
        if i in self._blobs:
            # read from modified/added blob
            blob = self._blobs[i].get_blob()
            data = b""
            while True:
                read = blob.read(self.DEFAULT_BLOB_READ)
                data += read
                if not read:
                    break
            return data
        else:
            # read from wrapped cluster
            return self._cluster.read_blob(adjusted_i)

    def iter_read_blob(self, i, buffersize=4096):
        adjusted_i = self._adjust_index(i)
        if i in self._blobs:
            # read from modified/added blob
            blob = self._blobs[i].get_blob()
            while True:
                read = blob.read(buffersize)
                if not read:
                    break
                yield read
        else:
            # read from wrapped cluster
            yield from self._cluster.iter_read_blob(adjusted_i, buffersize=buffersize)

    # ----- ModifiableMixIn methods -----

    def get_initial_disk_size(self):
        # initial disk size may differ from the size this object would
        # have if we were to write it to disk again
        # for example, different compression parameters may cause this
        # even when using the same compression type
        return self._cluster.get_total_compressed_size()

    def get_disk_size(self):
        # we can only know the compressed size by compressing it
        size = 0
        for chunk in self.iter_write():
            size += len(chunk)
        return size

    # ----- BindableMixIn methods -----

    def bind(self, zim):
        """
        Bind this object to a ZIM file.

        This method behaves mostly like L{pyzim.bindable.BindableMixIn.bind},
        but will also bind the wrapped cluster EXCEPT if it is already
        bound. This is so that the wrapped cluster and the wrapper can
        be bound to two different ZIM objects.

        @param zim: ZIM archive to bind to
        @type zim: L{pyzim.archive.Zim}
        @raise pyzim.exceptions.AlreadyBound: when already bound to a zim archive
        """
        # we need to also bind the wrapped cluster
        BindableMixIn.bind(self, zim)
        if not self._cluster.bound:
            self._cluster.bind(zim)

    def unbind(self):
        # we also need to unbind the wrapped cluster
        BindableMixIn.unbind(self)
        self._cluster.unbind()


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
