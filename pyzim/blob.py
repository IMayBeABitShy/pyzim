"""
Classes for working with blobs (content of entries).

We split this behavior in two main classes:

    - L{BaseBlobSource} instances are used to provide L{BaseBlob} instances
    - L{BaseBlob} instances provide the data that will be written to the ZIM file

The idea here is that L{BaseBlobSource} instances provide a resettable way
of generating L{BaseBlob} instances. Having the ability to generate a blob
multiple times may allow us approaches for an improved compression.
"""
import io
import os

from pyzim import constants


class BaseBlobSource(object):
    """
    Base class for blob sources.

    A BaseBlobSource provides L{BaseBlob} instances.
    """
    def get_blob(self):
        """
        Return a blob.

        @return: a fresh instance of a subclass of L{BaseBlob}
        @rtype: L{BaseBlob}
        """
        raise NotImplementedError("BaseBlobSource.get_blob() not implemented by subclass!")

    def get_size(self):
        """
        Get the size of a blob from this factory.

        The default implementation instantiates a new blob using
        L{BaseBlobSource.get_blob} and calls L{BaseBlob.get_size}.
        Subclasses may overwrite this method for a more efficient
        implementation.

        Please See L{BaseBlob.get_size} for details.

        @return: the size of a blob in byzes
        @rtype: L{int}
        """
        b = self.get_blob()
        size = b.get_size()
        b.close()
        return size


class BaseBlob(object):
    """
    Base class for blobs

    A Blob provides binary content to be written to a ZIM file.

    In a simplified manner, a BaseBlob behaves similiar to a file-like object.
    """
    def get_size(self):
        """
        Return the size of this blob.

        This value should never be lower than the actual size.

        @return: the size of this blob in bytes
        @rtype: L{int}
        """
        raise NotImplementedError("BaseBlob.get_size() not implemented by subclass!")

    def read(self, n):
        """
        Read up to n bytes from the blob.

        The "n" parameter indicates the max amount of bytes that should
        be returned. It is meant as a mere suggestion and can safely be
        ignored.

        This function should return an empty bytestring if all data has
        been read.

        @return: a chunk of data read from the blob
        @rtype: L{bytes}
        """
        raise NotImplementedError("BaseBlob.read() not implemented by subclass!")

    def close(self):
        """
        Called when this blob is no longer needed.

        Use this method to clean up any associated resources.
        """
        pass


# ================= implementations ==================


class InMemoryBlobSource(BaseBlobSource):
    """
    A L{BaseBlobSource} implementation for in-memory blobs.

    @ivar s: internal bytestring
    @type s: L{bytes}
    """
    def __init__(self, s, encoding=None):
        """
        The default constructor.

        @param s: the internal blob data.
        @type s: L{bytes} or L{str}
        @param encoding: if s is a unicode string, encode it with this encoding. Defaults to L{constants.ENCODING}.
        @type encoding: L{str} or L{None}
        """
        assert isinstance(s, (bytes, str))
        assert isinstance(encoding, str) or (encoding is None)
        if encoding is None:
            encoding = constants.ENCODING
        if isinstance(s, str):
            self.s = s.encode(encoding)
        else:
            self.s = s

    def get_blob(self):
        return InMemoryBlob(self.s)

    def get_size(self):
        # as we already know the size of the string, we can be slightly
        # more efficient
        return len(self.s)


class InMemoryBlob(BaseBlob):
    """
    A L{BaseBlob} used by L{InMemoryBlobSource} for in-memory blobs.

    @ivar _f: file-like object handling the data
    @type _f: L{io.BytesIO}
    @ivar _size: size of the blob
    @type _size: L{int}
    """
    def __init__(self, s):
        """
        The default constructor.

        @param s: blob data
        @type s: L{bytes}
        """
        assert isinstance(s, bytes)
        self._size = len(s)
        self._f = io.BytesIO(s)

    def get_size(self):
        return self._size

    def read(self, n):
        return self._f.read(n)

    def close(self):
        self._f.close()


class FileBlobSource(BaseBlobSource):
    """
    A L{BaseBlobSource} for reading a blob from a file.

    @ivar _path: path to file
    @type path: L{str}
    """
    def __init__(self, path):
        """
        The default constructor.

        @param path: path to file
        @type path: L{str}
        """
        assert isinstance(path, str)
        self._path = path

    def get_blob(self):
        return FileBlob(self._path)

    def get_size(self):
        # we can avoid opening the file
        return os.stat(self._path).st_size


class FileBlob(BaseBlob):
    """
    A L{BaseBlob} used by L{FileBlobSource} for file blobs.

    @ivar _f: file-like object handling the data
    @type _f: file-like
    @ivar _size: size of the file/blob
    @type _size: L{int}
    @ivar _path: path to file
    @type _path: L{str}
    """
    def __init__(self, path):
        """
        The default constructor.

        @param path: path to file
        @type path: L{str}
        """
        assert isinstance(path, str)
        self._path = path
        self._size = os.stat(self._path).st_size
        self._f = open(self._path, "rb")

    def get_size(self):
        return self._size

    def read(self, n):
        return self._f.read(n)

    def close(self):
        self._f.close()


class EmptyBlobSource(BaseBlobSource):
    """
    A L{BaseBlobSource} for empty blobs.

    This blob type is used for emptying ("soft deleting") blobs.
    """
    def __init__(self):
        """
        The default constructor.
        """
        pass

    def get_blob(self):
        return EmptyBlob()

    def get_size(self):
        return 0


class EmptyBlob(BaseBlob):
    """
    A L{BaseBlob} used by L{EmptyBlobSource} for empty blobs.
    """
    def __init__(self):
        """
        The default constructor.
        """
        pass

    def get_size(self):
        return 0

    def read(self, n):
        return b""

    def close(self):
        pass


class EntryBlobSource(BaseBlobSource):
    """
    A L{BaseBlobSource} that provides the content from an entry.

    This is useful if you want to create an item from an existing entry.

    NOTE: this BlobSource does not keep a copy of the entry in RAM.
    Instead, the content will be read as needed.
    """
    def __init__(self, entry):
        """
        The default constructor.

        @param entry: entry to get content from
        @type entry: L{pyzim.entry.ContentEntry}
        """
        self._entry = entry
        self._size = entry.get_size()

    def get_size(self):
        return self._size

    def get_blob(self):
        return EntryBlob(self._entry, size=self._size)


class EntryBlob(BaseBlob):
    """
    A L{BaseBlob} used by L{EntryBlobSource}.
    """
    def __init__(self, entry, size=None):
        """
        The default constructor.

        @param entry: entry to get content from
        @type entry: L{pyzim.entry.ContentEntry}
        @param size: size of the entry. Setting this improves performance.
        @type size: L{int} or L{None}
        """
        self._entry = entry
        if size is None:
            size = self._entry.get_size()
        self._size = size
        self._read_iter = None
        self._closed = False

    def get_size(self):
        return self._size

    def read(self, n):
        if self._closed:
            return b""
        if self._read_iter is None:
            self._read_iter = self._entry.iter_read()
        try:
            return next(self._read_iter)
        except StopIteration:
            # end of file
            self.close()
            return b""

    def close(self):
        self._closed = True
        if self._read_iter is not None:
            # finish reading so that the context exists gracefully
            for e in self._read_iter:
                pass
