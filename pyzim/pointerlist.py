"""
Implementation of URL and title pointer lists.
"""
import struct

from . import constants, exceptions


def binarysearch(to_search, element, key, start=0, end=None):
    """
    Adapted version of the binarysearch algorithm.
    See L{bisect.bisect_left} for general behavior notes.

    @param to_search: list/tuple to search. Must be sorted.
    @type to_search: L{list} or L{tuple}
    @param element: element to find
    @type element: any
    @param key: key function to extract comparison key. Not applied to element.
    @type key: callable expecting one argument
    @param start: start of search range. Used for recursive call.
    @type start: L{int}
    @param end: end of search range. Used for recursive call.
    @type end: L{int}
    @return: the first index for which all subsequent elements are >= the specified element
    @rtype: L{int}
    """
    assert isinstance(to_search, (list, tuple))
    assert isinstance(start, int) and start >= 0 and start < len(to_search)
    assert (isinstance(end, int) and end >= 0 and end < len(end)) or (end is None)
    assert (end is None) or (start <= end)
    if end is None:
        end = len(to_search)

    while start < end:
        mid = (start + end) // 2
        if key(to_search[mid]) < element:
            start = mid + 1
        else:
            end = mid
    return start


class SimplePointerList(object):
    """
    A pointer list of a ZIM file.

    A simple pointer list maps an index to a pointer.

    @cvar POINTER_FORMAT: format of a single pointer
    @type POINTER_FORMAT: L{str}
    """

    POINTER_FORMAT = "Q"

    def __init__(self, pointers):
        """
        The default constructor.

        @param pointers: list of pointers contained in this list
        @type pointers: L{list} of L{int}
        """
        assert isinstance(pointers, list)
        self._pointers = pointers

    @classmethod
    def from_bytes(cls, s):
        """
        Load a pointer list from the provided bytestring

        NOTE: it is not possible to validate that the string contains the whole pointer list.

        @param s: bytestring to parse
        @type s: L{bytes}
        @return: the pointerlist parsed from the bytes
        @rtype: L{pyzim.pointerlist.SimplePointerList}
        """
        pointer_size = struct.calcsize(constants.ENDIAN + cls.POINTER_FORMAT)
        length = len(s)
        if length % pointer_size != 0:
            raise ValueError(
                "Bytestring to parse into a pointer list must be a multiple of {}, got {}!".format(
                    pointer_size,
                    length,
                ),
            )
        n = length // pointer_size
        format = constants.ENDIAN + cls.POINTER_FORMAT * n
        pointer_list = list(struct.unpack(format, s))
        return cls(pointer_list)

    @classmethod
    def from_file(cls, f, n, seek=None):
        """
        Load a pointer list from a file.

        @param f: file-like object to read from
        @type f: file-like
        @param n: number of entries in the pointer list.
        @type n: L{int}
        @param seek: if specified, seek this position of the file before reading
        @type seek: L{int} or L{None}
        @return: the pointerlist read from the file
        @rtype: L{pyzim.pointerlist.SimplePointerList}
        """
        assert isinstance(n, int) and (n >= 0)
        assert isinstance(seek, int) or (seek is None)
        format = constants.ENDIAN + cls.POINTER_FORMAT * n
        if seek is not None:
            f.seek(seek)
        data = f.read(struct.calcsize(format))
        pointer_list = list(struct.unpack(format, data))
        return cls(pointer_list)

    def to_bytes(self):
        """
        Dump this pointer list into a bytestring and return it.

        @return: a bytestring describing this pointer list
        @rtype: L{bytes}
        """
        format = constants.ENDIAN + self.POINTER_FORMAT * len(self._pointers)
        return struct.pack(format, *self._pointers)

    def __len__(self):
        """
        The length of this pointer list.

        @return: the length of this pointer list
        @rtype: L{int}
        """
        return len(self._pointers)

    def get_by_index(self, i):
        """
        Return the pointer for the specified index.

        @param i: index of pointer to get
        @type i: L{int}
        @return: the poiner with the specified index
        @rtype: L{int}
        @raises IndexError: when the index is out of bounds.
        """
        assert isinstance(i, int)
        return self._pointers[i]

    def append(self, pointer):
        """
        Append a pointer to the end of pointerlist.

        If you are working with a L{pyzim.pointerlist.OrderedPointerList}, you will not want to use this method.
        Instead, use L{pyzim.pointerlist.OrderedPointerList.add}, which correctly inserts the pointer.

        @param pointer: pointer to add
        @type pointer: L{int}
        """
        assert isinstance(pointer, int) and pointer >= 0
        self._pointers.append(pointer)

    def set(self, i, pointer):
        """
        Set the pointer at the specified index.

        If you are working with a L{pyzim.pointerlist.OrderedPointerList}, you will not want to use this method.
        Instead, use L{pyzim.pointerlist.OrderedPointerList.add}, which correctly inserts the pointer.

        @param i: position of pointer to set
        @type i: l{int}
        @param pointer: new value for pointer
        @type pointer: L{int}
        @raise IndexError: on invalid index
        """
        assert isinstance(i, int) and i < len(self._pointers)
        assert isinstance(pointer, int) and pointer >= 0
        self._pointers[i] = pointer

    def iter_pointers(self, start=None, end=None):
        """
        Iterate over all pointers in this pointer list.

        If start and end are specified, they reference the indexes of the
        first (inclusive) and last (exclusive) pointers to return.
        In other words, this behavior matches the l[start:end] syntax.

        @param start: index of first pointer to return (inclusive)
        @type start: L{int}
        @param end: index of last pointer to return (exclusive)
        @type end: L{int}
        @yield: the pointers in the specified range
        @ytype: L{int}
        """
        if start is None:
            start = 0
        if end is None:
            end = len(self._pointers)
        assert isinstance(start, int) and start >= 0
        assert isinstance(end, int) and end <= len(self._pointers)
        assert end >= start

        for i in range(start, end):
            yield self._pointers[i]


class OrderedPointerList(SimplePointerList):
    """
    An ordered pointer list of a ZIM file.

    This is used by the URL pointer list and title pointer list.

    An ordered pointer list is used to map ordered entries to their
    locations. The order is determined by an external attribute (e.g.
    the URLs of the entry pointed at). The pointer list itself only
    contains the pointers, finding a pointer for a key requires the
    loading of the entries via the pointers.
    """

    def __init__(self, pointers, key_func):
        """
        The default constructor.

        @param pointers: list of pointers contained in this list
        @type pointers: L{list} of L{int}
        @param key_func: a function that returns the bytestring by which this pointer list is sorted.
        @type key_func: a callable returning L{bytes}
        """
        SimplePointerList.__init__(self, pointers)
        self._keyf = key_func

    @classmethod
    def from_bytes(cls, s, key_func):
        """
        Load a pointer list from the provided bytestring

        NOTE: it is not possible to validate that the string contains the whole pointer list.

        @param s: bytestring to parse
        @type s: L{bytes}
        @param key_func: a function that returns the bytestring by which this pointer list is sorted.
        @type key_func: a callable returning L{bytes}
        @return: the pointerlist parsed from the bytes
        @rtype: L{pyzim.pointerlist.OrderedPointerList}
        """
        pointer_size = struct.calcsize(constants.ENDIAN + cls.POINTER_FORMAT)
        length = len(s)
        if length % pointer_size != 0:
            raise ValueError(
                "Bytestring to parse into a pointer list must be a multiple of {}, got {}!".format(
                    pointer_size,
                    length,
                ),
            )
        n = length // pointer_size
        format = constants.ENDIAN + cls.POINTER_FORMAT * n
        pointer_list = list(struct.unpack(format, s))
        return cls(pointer_list, key_func=key_func)

    @classmethod
    def from_file(cls, f, n, key_func, seek=None):
        """
        Load a pointer list from a file.

        @param f: file-like object to read from
        @type f: file-like
        @param n: number of entries in the pointer list.
        @type n: L{int}
        @param key_func: a function that returns the bytestring by which this pointer list is sorted.
        @type key_func: a callable returning L{bytes}
        @param seek: if specified, seek this position of the file before reading
        @type seek: L{int} or L{None}
        @return: the pointerlist read from the file
        @rtype: L{pyzim.pointerlist.OrderedPointerList}
        """
        assert isinstance(n, int) and (n >= 0)
        assert isinstance(seek, int) or (seek is None)
        format = constants.ENDIAN + cls.POINTER_FORMAT * n
        if seek is not None:
            f.seek(seek)
        data = f.read(struct.calcsize(format))
        pointer_list = list(struct.unpack(format, data))
        return cls(pointer_list, key_func=key_func)

    def get(self, key):
        """
        Return the pointer for the specified key.

        The key must be in the same format as returned by the keyfunc.

        @param key: key to search for
        @type key: L{str} or L{bytes}
        @return: the pointer matching the key
        @rtype: L{int}
        @raises L{KeyError}: when no matching key was found.
        """
        assert isinstance(key, (str, bytes))
        if isinstance(key, str):
            key = key.encode(constants.ENCODING)
        i = binarysearch(self._pointers, key, key=self._keyf)
        if i != len(self._pointers) and self._keyf(self._pointers[i]) == key:
            return self._pointers[i]
        raise KeyError("No pointer matching key '{}' found!".format(key))

    def has(self, key):
        """
        Check if this pointer list has a pointer matching the key.

        This method calls L{OrderedPointerList.get} internally, so it's faster
        to not call this before get().

        @param key: key to check the presence of a matching pointer for
        @type key: L{bytes} or L{str}
        @return: True if the key is present, False otherwise
        @rtype: L{bool}
        """
        try:
            self.get(key)
        except KeyError:
            return False
        else:
            return True

    def add(self, key, pointer):
        """
        Add a key/pointer pair to this pointer list.

        @param key: key of pointer to add
        @type key: L{str} or L{bytes}
        @param pointer: pointer to add
        @type pointer: L{int}
        """
        assert isinstance(key, (str, bytes))
        assert isinstance(pointer, int) and (pointer >= 0)
        if isinstance(key, str):
            key = key.encode(constants.ENCODING)
        i = binarysearch(self._pointers, key, key=self._keyf)
        self._pointers.insert(i, pointer)

    def remove(self, key):
        """
        Remove a pointer from this pointer list.

        @param key: key of pointer to remove
        @type key: L{str} or L{bytes}
        @raises L{KeyError}: when no matching key was found.
        """
        assert isinstance(key, (bytes, str))
        if isinstance(key, str):
            key = key.encode(constants.ENCODING)
        i = binarysearch(self._pointers, key, key=self._keyf)
        if i != len(self._pointers) and self._keyf(self._pointers[i]) == key:
            del self._pointers[i]
        else:
            raise KeyError("No pointer matching key '{}' found!".format(key))

    def find_first_greater_equals(self, key):
        """
        Return the index of the first value greater or equal to the key.

        This is used as a utilty function to restrict the search
        to specific namespaces, but may have some other uses as well.

        The behavior mirrors L{bisect.bisect_left}.

        @param key: key to find the index of the next greater entry for
        @type key: L{str} or L{bytes}
        @return: the index of the first key/pointer larger than the entry
        @rtype: L{int}
        """
        assert isinstance(key, (str, bytes))
        if isinstance(key, str):
            key = key.encode(constants.ENCODING)
        return binarysearch(self._pointers, key, key=self._keyf)

    def check_sorted(self):
        """
        Check that this list is actually ordered correctly.

        @raises pyzim.exceptions.UnsortedList: when the pointers are not correctly ordered.
        """
        last_key = None
        for pointer in self._pointers:
            key = self._keyf(pointer)
            if (last_key is not None) and (last_key > key):
                # sort order violated
                raise exceptions.UnsortedList(
                    "{} contains at least two keys in wrong order: {} > {}".format(
                        self.__class__.__name__,
                        repr(last_key),
                        repr(key),
                    ),
                )
            last_key = key


class TitlePointerList(OrderedPointerList):
    """
    A pointer list used by the ZIM title listings.

    Unlike other pointer lists, these pointers do not refer to offsets
    but to entry IDs.
    """
    POINTER_FORMAT = "I"
