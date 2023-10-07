"""
Implementation of the MIME type list.
"""
import threading

from . import constants
from .modifiable import ModifiableMixIn
from .util.ioutil import read_until_zero


class MimeTypeList(ModifiableMixIn):
    """
    This class represents a MIME type list.

    The MIME type list contains an index->mimetype mapping. Each distinct
    mimetype used by entries is contained in the MIME type list. The
    entries themselves only contain the index of the MIME type in this
    list.

    @ivar _mimetypes: (ordered) list of mimetypes in this object
    @type _mimetypes: L{list} of L{bytes}
    @ivar _lock: thread safety lock
    @type _lock: L{threading.Lock}
    """
    def __init__(self, mimetypes):
        """
        The default constructor.

        @param mimetypes: list of mimetypes. Order matters!
        @type mimetypes: L{list} of L{bytes}
        """
        assert isinstance(mimetypes, list)
        ModifiableMixIn.__init__(self)
        self._mimetypes = mimetypes
        self._lock = threading.Lock()

        # ensure we know the current object size before modifications later
        self.after_flush_or_read()

    def __len__(self):
        """
        The length of this mimetype list.

        @return: the length of this mimetype list
        @rtype: L{int}
        """
        return len(self._mimetypes)

    def __str__(self):
        """
        Return a string displaying the content of the mimetypelist.

        @return: a string describing the content of the mimetypelist
        @rtype: L{str}
        """
        mimetypestrings = ["{:>4}. {}".format(i, mimetype.decode(constants.ENCODING)) for i, mimetype in enumerate(self._mimetypes)]
        return "\n".join(mimetypestrings) + "\n"

    @classmethod
    def from_file(cls, f, seek=None):
        """
        Read the mime type list from a file.

        @param f: file-like object to read from
        @type f: file-like
        @param seek: if specified, seek this position
        @type seek: L{int} or L{None}
        @return: the mimetypelist read from the file
        @rtype: L{pyzim.mimetypelist.MimeTypeList}
        """
        assert isinstance(seek, int) or seek is None
        if seek is not None:
            f.seek(seek)
        mimetypes = []
        while True:
            mimetype = read_until_zero(f)
            if not mimetype:
                # end of mimetype list
                break
            mimetypes.append(mimetype)
        return cls(mimetypes)

    def to_bytes(self):
        """
        Dump this mime type list into a a bytestring.

        @return: a bytestring containing this mimetypelist.
        @rtype: L{bytes}
        """
        return b"\x00".join(self._mimetypes) + b"\x00\x00"  # twice to indicate end of mimetype list

    def get(self, i, as_unicode=False):
        """
        Return the mimetype for the specified index.

        @param i: index of mimetype to get
        @type i: L{int}
        @param as_unicode: if nonzero, decode as unicode
        @type as_unicode: L{bool}
        @return: the mimetype, as bytes
        @rtype: L{bytes} or L{str} if as_unicode is nonzero
        """
        assert isinstance(i, int)
        if (i >= len(self._mimetypes)) or (i < 0):
            raise IndexError(
                "Mimetype index {} out ouf range {}!".format(
                    i,
                    len(self._mimetypes),
                ),
            )
        mimetype = self._mimetypes[i]
        if as_unicode:
            return mimetype.decode(constants.ENCODING)
        return mimetype

    def has(self, mimetype):
        """
        Check if this mimetypelist has the specified mimetype.

        @param mimetype: mimetype to check
        @type mimetype: L{bytes} or L{str}
        @return: True if the mimetypelist has the specified mimetype, False otherwise
        @rtype: L{bool}
        """
        assert isinstance(mimetype, (bytes, str))
        if isinstance(mimetype, str):
            mimetype = mimetype.encode(constants.ENCODING)
        return mimetype in self._mimetypes

    def get_index(self, mimetype, register=False):
        """
        Return the index for the specified mimetype.

        @param mimetype: mimetype to get index for
        @type mimetype: L{str} or L{bytes}
        @param register: if nonzero and the mimetype was not found, register it
        @type register: L{bool}
        @return: the index of the mimetype, None if not found and register is false
        @rtype: L{int} or L{None} if not found and register is not true
        """
        assert isinstance(mimetype, (bytes, str))
        if isinstance(mimetype, str):
            mimetype = mimetype.encode(constants.ENCODING)
        for i, element in enumerate(self._mimetypes):
            if element == mimetype:
                return i
        if register:
            self.register(mimetype)
            # recursively get new index
            return self.get_index(mimetype, register=False)
        else:
            return None

    def register(self, mimetype):
        """
        Register a mimetype in a thread-safe manner.

        Multiple registrations will be ignored, but should be avoided as
        the registration process in inefficient due to the lock.

        @param mimetype: mimetype to register
        @type mimetype: L{str} or L{bytes}
        @raises pyzim.exceptions.NonMutable: if mimetype list is not mutable
        """
        assert isinstance(mimetype, (bytes, str))
        self.ensure_mutable()
        if isinstance(mimetype, str):
            mimetype = mimetype.encode(constants.ENCODING)
        with self._lock:
            # first, check if we don't already have this mimetype registered
            # we do this even if this check has already been performed before
            # this way, we can ensure that multiple threads can not add the
            # same mime type twice
            if self.has(mimetype):
                return

            self._mimetypes.append(mimetype)
            self.mark_dirty()

    def iter_mimetypes(self, as_unicode=False):
        """
        Iterate over all mimetypes in this list.

        @param as_unicode: if nonzero, decode mimetypes
        @type as_unicode: L{bool}
        @yields: the mimetypes in this mimetype list
        @ytype: L{bytes} or L{str} if as_unicode is nonzero
        """
        for mimetype in self._mimetypes:
            if as_unicode:
                mimetype = mimetype.decode(constants.ENCODING)
            yield mimetype

    def get_disk_size(self):
        size = 1  # for end byte
        for mt in self._mimetypes:
            size += (len(mt) + 1)
        return size


if __name__ == "__main__":  # pragma: no cover
    # test script
    import argparse

    from .header import Header

    parser = argparse.ArgumentParser(description="Print the mimetypes in a ZIM file")
    parser.add_argument("path", help="path to file to read")
    ns = parser.parse_args()

    with open(ns.path, "rb") as fin:
        header = Header.from_file(fin)
        mimetypelist = MimeTypeList.from_file(fin, seek=header.mime_list_position)
        print(str(mimetypelist))
