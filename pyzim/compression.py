"""
This module contains functionality related to compression.
"""
import enum

try:
    import zlib
except ImportError:  # pragma: no cover
    zlib = None
try:
    import lzma
except ImportError:  # pragma: no cover
    lzma = None
try:
    import bz2
except ImportError:  # pragma: no cover
    bz2 = None
try:
    import pyzstd
except ImportError:  # pragma: no cover
    pyzstd = None

from .exceptions import UnsupportedCompressionType


class CompressionType(enum.IntEnum):
    """
    This enum describes the type of compression used.

    The values of each element match the values in the ZIM cluster information bytes.
    """
    UNKNOWN = -1
    NONE = 1
    ZLIB = 2
    BZ2 = 3
    LZMA2 = 4
    ZSTD = 5


class CompressionTarget(enum.IntEnum):
    """
    This enum provides some symbolic constants for compression targets.

    The compression target specifies how compression should be optimized.
    For example, we may want to configure a compression algorithm for
    maximal compression ratio or configure it for fastest decompression.

    @cvar ANY: let the L{BaseCompressionInterface} choose the options
    @cvar MAX_COMPRESSION: optimize for maximal compression
    @cvar REASONABLE_COMPRESSION: optimize for maximal reasonable compression (e.g. avoid ultra zstd compression)
    @cvar FASTEST_COMPRESSION: optimize for fastest compression
    @cvar FASTEST_DECOMPRESSION: optimize for fastest decompression
    @cvar LOWRAM_DECOMPRESSION: optimize for compression in low-RAM environments
    @cvar LOWRAM_DECOMPRESSION: optimize for decompression in low-RAM environments
    @cvar BALANCED: choose a reasonable performance-compression-ratio
    """
    ANY = 0
    MAX_COMPRESSION = 1
    REASONABLE_COMPRESSION = 2
    FASTEST_COMPRESSION = 3
    FASTEST_DECOMPRESSION = 4
    LOWRAM_COMPRESSION = 5
    LOWRAM_DECOMPRESSION = 6
    BALANCED = 7


class BaseCompressionInterface(object):
    """
    Base class for compression interfaces.

    A CompressionInterface provides a set of methods to compress and
    decompress data in a standardized format.

    This interface is based on compressor-like and decompressor-like objects.
    A compressor-like object (e.g. L{lzma.LZMACompressor}) will be used
    to compress data while a decompressor-like object (e.g. L{lzma.LZMADecompressor})
    will be used to decompress the data without storing it in memory all
    at once.

    The get_(de)compressor() methods take an 'options' argument. This
    argument is a dictionary providing further option settings for the
    (de-)compressor. These options are mostly compression-type dependent
    and are not standardized. It is recommended to name the options "type.option",
    e.g. "lzma.check". Unknown options should be ignored.

    Additionally, the following options are recommended to be implemented:

        - general.target: target to optimize compression for (e.g. performance, compression).

    @cvar compression_type: compression type used by this interface
    @type compression_type: L{pyzim.compression.CompressionType}
    """

    compression_type = CompressionType.UNKNOWN

    @staticmethod
    def get_compressor(options={}):  # pragma: no cover
        """
        Return a compressor object which can be used to compress data.

        @param options: additional options for the compressor.
        @type options: L{dict}
        @return: a compressor object
        @rtype: a compressor-like object. See L{pyzim.compression.BaseCompressionInterface} for more info.
        """
        raise NotImplementedError("get_compressor() not implemented for this compression type!")

    @staticmethod
    def get_decompressor(options={}):  # pragma: no cover
        """
        Return a decompressor object which can be used to decompress data.

        @param options: additional options for the decompressor.
        @type options: L{dict}
        @return: a decompressor object
        @rtype: a decompressor-like object. See L{pyzim.compression.BaseCompressionInterface} for more info.
        """
        raise NotImplementedError("get_compressor() not implemented for this compression type!")


class CompressionRegistry(object):
    """
    The CompressionRegistry manages the mapping of L{pyzim.compression.CompressionType} -> L{pyzim.compression.BaseCompressionInterface}.

    It allows compression interfaces to dynamically register themselves to this class.
    Multiple interfaces can be registered for each compression type.
    """

    _interfaces = {}

    def __init__(self):
        """
        A fake constructor. Do not instantiate this class.

        @raises RuntimeError: always
        """
        raise RuntimeError("{} should not be instantiated!".format(self.__class__))

    @classmethod
    def register(cls, compression_type, interface):
        """
        Register a new interface for the specified compression type.

        @param compression_type: compression type to register the interface for.
        @type compression_type: L{pyzim.compression.CompressionType}
        @param interface: the interface that should be registered.
        @type interface: a subclass of L{pyzim.compression.BaseCompressionInterface}
        """
        assert isinstance(compression_type, CompressionType)
        assert issubclass(interface, BaseCompressionInterface)
        if compression_type not in cls._interfaces:
            cls._interfaces[compression_type] = [interface]
        else:
            cls._interfaces[compression_type].append(interface)

    @classmethod
    def unregister(cls, compression_type, interface):
        """
        Unregister a interface for the specified compression type.

        If compression_type is None, unregister for all interfaces.
        If interface is None, unregister all interfaces for the compression type.

        @param compression_type: compression type to unregister the interface from.
        @type compression_type: L{pyzim.compression.CompressionType} or L{None}
        @param interface: the interface that should be unregistered.
        @type interface: a subclass of L{pyzim.compression.BaseCompressionInterface} or L{None}
        """
        assert (compression_type is None) or isinstance(compression_type, CompressionType)
        assert (interface is None) or issubclass(interface, BaseCompressionInterface)
        if compression_type is None:
            compression_types = list(cls._interfaces.keys())
        else:
            compression_types = [compression_type]
        for ct in compression_types:
            if interface is None:
                del cls._interfaces[ct]
            elif interface in cls._interfaces[ct]:
                cls._interfaces[ct].remove(interface)

    @classmethod
    def get(cls, compression_type):
        """
        Return a compression interface for the specified compression type.


        @param compression_type: compression type to get an interface for.
        @type compression_type: L{pyzim.compression.CompressionType}
        @return: the interface for that compression type
        @rtype: a subclass of L{pyzim.compression.BaseCompressionInterface}
        @raises pyzim.exceptions.UnsupportedCompressionType: if no compression interface know the specified compression type.
        """
        assert isinstance(compression_type, CompressionType)
        if compression_type in cls._interfaces:
            return cls._interfaces[compression_type][0]
        raise UnsupportedCompressionType("No known compression interface for compression type '{}'.".format(compression_type))

    @classmethod
    def has(cls, compression_type):
        """
        Check if a compression interface has been registered for the specfied compression type.

        @param compression_type: compression type to check.
        @type compression_type: L{pyzim.compression.CompressionType}
        @return: True if an interface has been registered for the specified compression type
        @rtype: L{bool}
        """
        assert isinstance(compression_type, CompressionType)
        return compression_type in cls._interfaces

    @classmethod
    def iter_for(cls, compression_type):
        """
        Iterate over each registered compression interface for the specified compression type.

        @yields: the registered compression interfaces for the specified compression type
        @ytype: a subclass of L{pyzim.compression.BaseCompressionInterface}
        """
        assert isinstance(compression_type, CompressionType)
        interfaces = cls._interfaces.get(compression_type, [])
        for interface in interfaces:
            yield interface


# ================= helpers ===================


class DecompressingReader(object):
    """
    A helper class that helps with reading data selectively from a compressed stream.

    @ivar read_decompressed: number of decompressed bytes read
    @type read_decompressed: L{int}
    @ivar read_compressed: number of compressed bytes read.
    @type read_compressed: L{int}
    """

    def __init__(self, f, decompressor):
        """
        The default constructor.

        @param f: file to read from
        @type f: file-like
        @param decompressor: a decompressor-like object to decompress the data
        @type decompressor: decompressor-like
        """
        self._f = f
        self._decompressor = decompressor
        self._buffer = b""
        self.read_decompressed = 0
        self.read_compressed = 0

    @property
    def total_compressed_size(self):
        """
        Return the total size of the compressed data stream so far.

        This only works if the compressor has read until the end.

        @return: the total number of compressed bytes read minus the read bytes that were not part of the compressed stream.
        @rtype: L{int}
        """
        if not self._decompressor.eof:
            raise RuntimeError("DecompressingReader.total_compressed_size() can only be called after the full compressed stream has been read!")
        return self.read_compressed - len(self._decompressor.unused_data)

    def read(self, n, extra_decompress=0):
        """
        Read up to n bytes.

        Similiar to file-like objects, this may return less bytes, but
        will return an empy string if and only if there's nothing left
        to read.

        @param n: maximum number of bytes to read
        @type n: L{int}
        @param extra_decompress: decompress this many bytes more, mainly used for testing
        @type extra_decompress: L{int}
        @return: the bytes read
        @rtype: L{str}
        """
        assert isinstance(n, int) and n >= 0
        assert isinstance(extra_decompress, int) and extra_decompress >= 0

        # not all decompressors can guarantee that they decompress at
        # most n bytes. Thus, we need an internal buffer to catch any
        # extra bytes.
        # The extra_decompress argument can be used to increase the
        # number of bytes decompressed. This may have efficiency benefits,
        # but is mostly used in testing to ensure that the required buffer
        # code also works

        if self._buffer:
            # can still return from internal buffer
            to_read = min(n, len(self._buffer))
            ret = self._buffer[:to_read]
            self._buffer = self._buffer[to_read:]
            self.read_decompressed += to_read
            return ret
        elif self._decompressor.eof:
            return b""
        else:
            # may need to decompress further data
            while True:
                # loop because f.read() may not produce enough data per
                # call to decompress further data
                if self._decompressor.needs_input:
                    further_data = self._f.read(n)
                    if not further_data:
                        # EOF
                        raise IOError("EOF during decompression")
                    self.read_compressed += len(further_data)
                else:
                    further_data = b""
                decompressed = self._decompressor.decompress(
                    further_data,
                    max_length=(n + extra_decompress),
                )
                if (not decompressed) and self._decompressor.needs_input:
                    continue
                else:
                    break
            if len(decompressed) > n:
                self._buffer += decompressed[n:]
                ret = decompressed[:n]
                self.read_decompressed += len(ret)
                return ret
            self.read_decompressed += len(decompressed)
            return decompressed

    def read_n(self, n):
        """
        Read until n bytes have been read (or until EOF has been encountered).

        This method will attempt to always return exactly n bytes. However,
        should the EOF be encountered before this, less bytes may be returned.

        @param n: number of bytes to read
        @type n: L{int}
        @return: the bytes read
        @rtype: L{str}
        """
        data = b""
        while len(data) < n:
            new_data = self.read(n - len(data))
            if not new_data:
                break
            data += new_data
        return data

    def iter_read(self, n=-1, buffersize=4096):
        """
        Read n bytes iteratively.

        @param n: number of bytes to read. If smaller than 0, read until EOF.
        @type n: L{int}
        @param buffersize: size of chunks to read at once
        @type buffersize: L{int}
        @yields: the data read (may be less than n)
        @ytype: L{bytes}
        """
        assert isinstance(n, int)
        assert isinstance(buffersize, int) and buffersize > 0

        n_read = 0
        while True:
            if n >= 0:
                # also check if we've read enough data
                if n_read >= n:
                    break
                to_read = min(buffersize, n - n_read)
            else:
                to_read = buffersize
            data = self.read(to_read)
            if not data:
                break
            yield data
            n_read += len(data)

    def skip(self, n):
        """
        Skip the next n bytes.

        This will also increase L{DecompressingReader.read_compressed} and L{DecompressingReader.read_decompressed}.

        @param n: number of bytes to skip
        @type n: L{int}
        """
        assert isinstance(n, int) and n >= 0
        n_skipped = 0
        while n_skipped < n:
            to_read = n - n_skipped
            data = self.read(to_read)
            if not data:
                # no data left, exit early.
                return
            n_skipped += len(data)

    def skip_to(self, offset):
        """
        Skip bytes until the specified offset is reached.

        @param offset: offset to skip to
        @type offset: L{int}
        @raises IOError: if offset points behind current position
        """
        assert isinstance(offset, int) and offset >= 0
        cur_pos = self.read_decompressed
        if cur_pos > offset:
            raise IOError("Can not skip to {}, already at {}!".format(offset, cur_pos))
        to_skip = offset - cur_pos
        self.skip(to_skip)

    def reseek(self, base_offset=0):
        """
        Re-seek the wrapped file object to match the last read position.

        This should be called whenever some other code may have read/seeked
        the underlying file object.

        @param base_offset: If specified, take this offset within the underlying file object as the actual start of this wrapper.
        @type base_offset: L{int}
        """
        to_seek = base_offset + self.read_compressed
        self._f.seek(to_seek)


# ================= implementations =====================


class PassthroughCompressor(object):
    """
    A compressor-like object that does not compress.

    Should be thread safe.
    """
    def __init__(self):
        self._flush_called = False

    def compress(self, data):
        """
        "Compress" the data, returning the "compressed" data.

        @param data: data to "compress"
        @type data: L{bytes}
        @return: the  original, uncompressed data
        @rtype: L{bytes}
        """
        if not isinstance(data, bytes):
            raise TypeError("Can only compress bytes, not {}".format(type(data)))
        return data

    def flush(self):
        """
        Finish the compression, returning all remaing "compressed" data.

        @return: an empty bytestring
        @rtype: L{bytes}
        @raise ValueError: when called more than once.
        """
        if self._flush_called:
            raise ValueError("Repeated call to flush()")
        self._flush_called = True
        return b""


class PassthroughDecompressor(object):
    """
    A decompressor-like object that does not decompress.

    @ivar unused_data: data found after the end of the data stream. Always empty.
    @type unused_data: L{bytes}
    """
    def __init__(self):
        self._buffer = b""
        self.unused_data = b""

    @property
    def needs_input(self):
        """
        False if the decompress() method can provide more decompressed
        data before requiring new input.

        @return: whether more input is needed for the next decompress() call
        @rtype: L{bool}
        """
        return len(self._buffer) == 0

    def decompress(self, data, max_length=-1):
        """
        Decompress the data. Some data may be buffered internally.

        @param data: data to decompress
        @type data: L{bytes}
        @param max_length: if positive, return at most this many bytes
        @type max_length: L{int}
        @return: the decompressed data
        @rtype: L{bool}
        """
        if not isinstance(data, bytes):
            raise TypeError("Can only decompress bytes, not {}".format(type(data)))
        if not isinstance(max_length, int):
            raise TypeError("max_length must be an integer!")

        full_data = self._buffer + data
        if max_length < 0:
            self._buffer = b""
            return full_data
        else:
            to_read = min(max_length, len(full_data))
            self._buffer = full_data[to_read:]
            return full_data[:to_read]

    @property
    def eof(self):
        """
        True if the end of the stream has been reached.

        This will always be False, as we don't know when the end of the
        stream has been reached.
        """
        return False


class PassthroughCompressionInterface(BaseCompressionInterface):
    """
    A L{pyzim.compression.BaseCompressionInterface} for no compression.
    """

    compression_type = CompressionType.NONE

    @staticmethod
    def get_compressor(options={}):
        return PassthroughCompressor()

    @staticmethod
    def get_decompressor(options={}):
        return PassthroughDecompressor()


CompressionRegistry.register(CompressionType.NONE, PassthroughCompressionInterface)


class LzmaCompressionInterface(BaseCompressionInterface):
    """
    A L{pyzim.compression.BaseCompressionInterface} for lzma compression.

    The following options are supported:

        - lzma.format (default: C{lzma.FORMAT_XZ} for compression, C{lzma.FORMAT_AUTO} for decompression): lzma format to use
        - lzma.check (default: C{lzma.CHECK_NONE}): integrity check to use (only for compression)
        - lzma.filters: (default: L{None}): custom filters to use
        - lzma.memlimit: memory limit for decompression (default: L{None})
    """

    compression_type = CompressionType.LZMA2

    @staticmethod
    def get_compressor(options={}):
        compression_target = options.get("general.target", CompressionTarget.ANY)
        preset = lzma.PRESET_DEFAULT
        if compression_target in (CompressionTarget.FASTEST_COMPRESSION, CompressionTarget.LOWRAM_DECOMPRESSION, CompressionTarget.LOWRAM_COMPRESSION):
            preset = 1
        elif compression_target == CompressionTarget.FASTEST_DECOMPRESSION:
            preset = 9
        elif compression_target == CompressionTarget.REASONABLE_COMPRESSION:
            preset = 9
        elif compression_target == CompressionTarget.MAX_COMPRESSION:
            preset = 9 | lzma.PRESET_EXTREME
        format = options.get("lzma.format", lzma.FORMAT_XZ)
        filters = options.get("lzma.filters", None)
        check = options.get("lzma.check", -1)
        return lzma.LZMACompressor(
            format=format,
            check=check,
            preset=preset,
            filters=filters,
            )

    @staticmethod
    def get_decompressor(options={}):
        format = options.get("lzma.format", lzma.FORMAT_AUTO)
        filters = options.get("lzma.filters", None)
        memlimit = options.get("lzma.memlimit", None)
        return lzma.LZMADecompressor(format=format, memlimit=memlimit, filters=filters)


if lzma is not None:  # pragma: no cover
    CompressionRegistry.register(CompressionType.LZMA2, LzmaCompressionInterface)


class Bz2CompressionInterface(BaseCompressionInterface):
    """
    A L{pyzim.compression.BaseCompressionInterface} for lzma compression.
    """

    compression_type = CompressionType.BZ2

    @staticmethod
    def get_compressor(options={}):
        compression_target = options.get("general.target", CompressionTarget.ANY)
        level = 9
        if compression_target == CompressionTarget.FASTEST_COMPRESSION:
            # no significant performance differnece up until level 6
            level = 5
        elif compression_target == CompressionTarget.FASTEST_DECOMPRESSION:
            level = 1
        elif compression_target in (CompressionTarget.LOWRAM_DECOMPRESSION, CompressionTarget.LOWRAM_COMPRESSION):
            level = 1
        elif compression_target in (CompressionTarget.REASONABLE_COMPRESSION, CompressionTarget.MAX_COMPRESSION):
            level = 9
        return bz2.BZ2Compressor(level)

    @staticmethod
    def get_decompressor(options={}):
        return bz2.BZ2Decompressor()


if bz2 is not None:  # pragma: no cover
    CompressionRegistry.register(CompressionType.BZ2, Bz2CompressionInterface)


class ZlibDecompressorWrapper(object):
    """
    A wrapper around L{zlib.decompressobj} to provide additional attributes.
    """

    def __init__(self, wbits=None, zdict=None):
        """
        The default constructor.

        @param wbits: window size to use
        @type wbits: L{int} or L{None}
        @param zdict: compression dictionary to use (must be the same as used by the compressor)
        @type zdict: L{bytes} or L{None}
        """
        assert isinstance(wbits, int) or wbits is None
        assert isinstance(zdict, bytes) or zdict is None
        if wbits is None:
            wbits = zlib.MAX_WBITS
        kwargs = {}  # see ZlibCompressionInterface
        if zdict is not None:
            kwargs["zdict"] = zdict
        self._decompressor = zlib.decompressobj(wbits=wbits, **kwargs)
        self._buffer = b""

    def decompress(self, data, max_length=-1):
        """
        Return a bytes object containing the decompressed version of the data.

        @param data: binary data to decompress
        @type data: L{bytes}
        @param max_length: The maximum allowable length of the decompressed data.
        @type max_length: L{int}
        """
        if max_length < 0:
            # workaround for a non-negative bug
            to_read = 0
        else:
            blength = len(self._buffer)
            to_read = max(0, max_length - blength)
        data = self._decompressor.unconsumed_tail + data
        self._buffer += self._decompressor.decompress(data, max_length=to_read)
        if max_length < 0:
            tdata = self._buffer
            self._buffer = b""
            return tdata
        else:
            to_split = min(max_length, len(self._buffer))
            tdata = self._buffer[:to_split]
            self._buffer = self._buffer[to_split:]
            return tdata

    @property
    def needs_input(self):
        """
        True if additional input is needed before more data can be decompressed.

        @return: whether additional input data is needed to decompress further data.
        @rtype: L{bool}
        """
        if self._buffer:
            return False
        if self.eof:
            return False
        if self._decompressor.unconsumed_tail:
            return False
        return True

    @property
    def unused_data(self):
        """
        Data found after the end of the compressed stream.

        @return: data found after the end of the compressed stream
        @rtype: L{bytes}
        """
        if self._buffer:
            t = self._buffer
            self._buffer = b""
            return t
        return self._decompressor.unused_data

    @property
    def eof(self):
        """
        True if the end of the data stream has been reached.

        @return: whether the end of the data stream has been reached or not
        @rtype: L{bool}
        """
        return self._decompressor.eof


class ZlibCompressionInterface(BaseCompressionInterface):
    """
    A L{pyzim.compression.BaseCompressionInterface} for zlib compression.

    The following options are supported:

        - zlib.memlevel (default: C{zlib.DEF_MEM_LEVEL}, set automatically when required by target): zlib memory level to use (only for compression)
        - zlib.wbits (default: C{zlib.MAX_WBITS}, set automatically when required by target):  zlib window size to use
        - zlib.method (default: C{zlib.DEFLATED}): compression algorithm to use (only for compression)
        - zlib.strategy (default: C{Z_DEFAULT_STRATEGY}): zlib strategy for the compression algorithm (only for compression)
        - zdict (default: L{None}): predefined compression dictionary to use.
    """

    compression_type = CompressionType.ZLIB

    @staticmethod
    def get_compressor(options={}):
        compression_target = options.get("general.target", CompressionTarget.ANY)
        memlevel = options.get("zlib.memlevel", None)
        wbits = options.get("zlib.wbits", None)
        level = zlib.Z_DEFAULT_COMPRESSION
        if compression_target == CompressionTarget.FASTEST_COMPRESSION:
            level = 1
        if compression_target in (
            CompressionTarget.FASTEST_DECOMPRESSION,
            CompressionTarget.LOWRAM_DECOMPRESSION,
            CompressionTarget.MAX_COMPRESSION,
            CompressionTarget.REASONABLE_COMPRESSION,
        ):
            level = 9
            if memlevel is None:
                memlevel = 9
            if wbits is None:
                wbits = zlib.MAX_WBITS
        elif compression_target == CompressionTarget.LOWRAM_COMPRESSION:
            level = 9
            if memlevel is None:
                memlevel = 1
            if wbits is None:
                wbits = 9
        if memlevel is None:
            memlevel = zlib.DEF_MEM_LEVEL
        if wbits is None:
            wbits = zlib.MAX_WBITS
        method = options.get("zlib.method", zlib.DEFLATED)
        strategy = options.get("zlib.strategy", zlib.Z_DEFAULT_STRATEGY)
        zdict = options.get("zlib.zdict", None)
        # workaround because the we can not pass zdict as an argument at
        # all when it has not been supplied.
        kwargs = {}
        if zdict is not None:
            kwargs["zdict"] = zdict
        return zlib.compressobj(
            level=level,
            method=method,
            wbits=wbits,
            memLevel=memlevel,
            strategy=strategy,
            **kwargs,
        )

    @staticmethod
    def get_decompressor(options={}):
        zdict = options.get("zlib.zdict", None)
        wbits = options.get("zlib.wbits", zlib.MAX_WBITS)
        return ZlibDecompressorWrapper(wbits=wbits, zdict=zdict)


if zlib is not None:  # pragma: no cover
    CompressionRegistry.register(CompressionType.ZLIB, ZlibCompressionInterface)


class PyZstdCompressionInterface(BaseCompressionInterface):
    """
    A L{pyzim.compression.BaseCompressionInterface} for zstd compression using C{pyzstd}.

    The following options are supported:

        - zstd.option: a zstd level (L{int}) or advanced compression parameters (L{dict})
        - zstd.dict: pre-trained dictionary for compression (type C{pyzstd.ZstdDict})
    """

    compression_type = CompressionType.ZSTD

    @staticmethod
    def get_compressor(options={}):
        level_or_option = options.get("zstd.option", None)
        zdict = options.get("zstd.dict", None)
        target = options.get("general.target", CompressionTarget.ANY)
        if level_or_option is None:
            if target in (CompressionTarget.REASONABLE_COMPRESSION, ):
                level_or_option = 19
            elif target in (CompressionTarget.MAX_COMPRESSION, CompressionTarget.FASTEST_DECOMPRESSION):
                level_or_option = 22
            elif target in (CompressionTarget.FASTEST_COMPRESSION, CompressionTarget.FASTEST_DECOMPRESSION):
                level_or_option = 2
        return pyzstd.ZstdCompressor(level_or_option=level_or_option, zstd_dict=zdict)

    @staticmethod
    def get_decompressor(options={}):
        option = options.get("zstd.option", None)
        zdict = options.get("zstd.dict", None)
        return pyzstd.ZstdDecompressor(zstd_dict=zdict, option=option)


if pyzstd is not None:  # pragma: no cover
    CompressionRegistry.register(CompressionType.ZSTD, PyZstdCompressionInterface)
