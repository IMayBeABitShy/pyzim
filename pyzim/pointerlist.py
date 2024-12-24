"""
Implementation of URL and title pointer lists.
"""
import struct
import threading

from . import constants, exceptions
from .modifiable import ModifiableMixIn


# ============ HELPER FUNCTIONS =============

def binarysearch(to_search, element, key, start=0, end=None):
    """
    Adapted version of the binarysearch algorithm.
    See L{bisect.bisect_left} for general behavior notes.

    @param to_search: a sorted, indexable object to search
    @type to_search: a sorted, indexable object
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
    assert hasattr(to_search, "__getitem__")
    assert isinstance(start, int) and start >= 0 and start <= len(to_search)
    assert (isinstance(end, int) and end >= 0 and end <= len(end)) or (end is None)
    assert (end is None) or (start <= end)
    if end is None:
        end = len(to_search)
    if end - start == 0:
        # only insertion point is at start
        return start

    while start < end:
        mid = (start + end) // 2
        if key(to_search[mid]) < element:
            start = mid + 1
        else:
            end = mid
    return start


# ============ BASE POINTER LISTS =============


class SimplePointerList(ModifiableMixIn):
    """
    A pointer list of a ZIM file.

    A simple pointer list maps an index to a pointer.

    @cvar POINTER_FORMAT: format of a single pointer
    @type POINTER_FORMAT: L{str}

    @ivar _pointers: list of pointers in this pointer list
    @type _pointers: L{list} of L{int}
    @ivar _lock_ thread safety lock
    @type _lock: L{threading.Lock}
    """

    POINTER_FORMAT = "Q"

    def __init__(self, pointers):
        """
        The default constructor.

        @param pointers: list of pointers contained in this list
        @type pointers: L{list} of L{int}
        """
        assert isinstance(pointers, list)
        ModifiableMixIn.__init__(self)
        self._pointers = pointers
        self._lock = threading.Lock()

        # ensure we know the current object size before modifications later
        self.after_flush_or_read()

    @classmethod
    def new(cls):
        """
        Instantiate a new, empty pointerlist.

        @return: a new, empty pointerlist
        @rtype: L{SimplePointerList}
        """
        return cls([])

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

    @classmethod
    def from_zim_file(cls, zim, n, seek=None):
        """
        Load a pointer list from a ZIM file at the specified offset.

        This usually acquires the file lock and calls L{SimplePointerList.from_file},
        but subclasses may behave differently.

        @param zim: ZIM file to read from
        @type zim: L{pyzim.archive.Zim}
        @param n: number of entries in the pointer list.
        @type n: L{int}
        @param seek: if specified, seek this position of the file before reading
        @type seek: L{int} or L{None}
        @return: the pointerlist read from the file
        @rtype: L{pyzim.pointerlist.SimplePointerList}
        """
        assert isinstance(n, int) and (n >= 0)
        assert isinstance(seek, int) or (seek is None)
        with zim.acquire_file() as f:
            return cls.from_file(f=f, n=n, seek=seek)

    @classmethod
    def from_zim_entry(cls, zim, full_url):
        """
        Load a pointer list from an entry inside a ZIM archive.

        NOTE: it is not possible to validate that the string contains the whole pointer list.

        @param zim: ZIM file to read from
        @type zim: L{pyzim.archive.Zim}
        @param full_url: full url of entry to read pointerlist from
        @type full_url: L{str}
        @return: the pointerlist parsed from the bytes
        @rtype: L{pyzim.pointerlist.SimplePointerList}
        """
        entry = zim.get_entry_by_full_url(full_url).resolve()
        return cls.from_bytes(entry.read())

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

    def __getitem__(self, i):
        """
        Get the pointer at the specified index.

        This is basically just a wrapper around L{SimplePointerList.get_by_index}.

        @param i: index of pointer to get
        @type i: L{int}
        @return: the poiner with the specified index
        @rtype: L{int}
        @raises IndexError: when the index is out of bounds.
        """
        return self.get_by_index(i)

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

    def get_by_pointer(self, pointer):
        """
        Return the index of pointer in this list.

        @param pointer: the pointer to find index of
        @type pointer: L{int}
        @return: the index of this pointer
        @rtype: L{int}
        @raises KeyError: if pointer not found in archive
        """
        assert isinstance(pointer, int)
        for i in range(len(self)):
            listpointer = self._pointers[i]
            if listpointer == pointer:
                return i
        raise KeyError("Pointer {} not found in pointer list!".format(pointer))

    def append(self, pointer):
        """
        Append a pointer to the end of pointerlist.

        If you are working with a L{pyzim.pointerlist.OrderedPointerList}, you will not want to use this method.
        Instead, use L{pyzim.pointerlist.OrderedPointerList.add}, which correctly inserts the pointer.

        @param pointer: pointer to add
        @type pointer: L{int}
        @raises pyzim.exceptions.NonMutable: if pointer list is not mutable
        """
        assert isinstance(pointer, int) and pointer >= 0
        self.ensure_mutable()
        with self._lock:
            self._pointers.append(pointer)
            self.mark_dirty()

    def set(self, i, pointer, add_placeholders=False):
        """
        Set the pointer at the specified index.

        If C{i == len(self)}, append the pointer. IF C{i > len(self)},
        this will raise a L{IndexError}, unless you also set C{add_placeholders = True}.

        If you are working with a L{pyzim.pointerlist.OrderedPointerList}, you will not want to use this method.
        Instead, use L{pyzim.pointerlist.OrderedPointerList.add}, which correctly inserts the pointer.

        @param i: position of pointer to set
        @type i: l{int}
        @param pointer: new value for pointer
        @type pointer: L{int}
        @param add_placeholders: if nonzero, allow setting i>len(self), adding placeholders
        @type add_placeholders: L{bool}
        @raise IndexError: on invalid index
        @raises pyzim.exceptions.NonMutable: if pointer list is not mutable
        """
        assert isinstance(i, int)
        assert isinstance(pointer, int) and pointer >= 0
        assert isinstance(add_placeholders, bool)
        if (i > len(self._pointers)) and (not add_placeholders):
            raise IndexError("Setting index would leave gaps and add_placeholders is not nonzero!")
        self.ensure_mutable()
        with self._lock:
            length = len(self._pointers)
            if i < length:
                self._pointers[i] = pointer
            elif i == length:
                self._pointers.append(pointer)
            else:
                # add placeholders
                # placeholders should always point to a valid target
                # so let's just use the same pointer as well
                to_add = i - length + 1  # 1 for each missing and 1 for the target itself
                for i in range(to_add):
                    self._pointers.append(pointer)
            self.mark_dirty()

    def remove_by_index(self, i):
        """
        Remove the pointer at the specified index.

        @param i: index of pointer to remove
        @type i: L{int}
        @raises IndexError: on invalid index
        """
        assert isinstance(i, int) and i >= 0
        self.ensure_mutable()
        del self._pointers[i]
        self.mark_dirty()

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
            end = len(self)
        assert isinstance(start, int) and start >= 0
        assert isinstance(end, int) and end <= len(self)
        assert end >= start

        for i in range(start, end):
            yield self.get_by_index(i)

    def mass_update(self, diff, start=None, end=None):
        """
        Perform a mass update on all pointers in the specified range, changing their value as specified.

        Each pointer is changed by the 'diff' value.

        IMPORTANT: Unlike L{SimplePointerList.iter_pointers}, start and
        end of this method refer to the pointer values, not the indexes.

        If start and end are specified, they reference the pointers of the
        first (inclusive) and last (exclusive) pointers to modify.
        In other words, this behavior matches the l[start:end] syntax.

        @param diff: value to add to each pointer
        @type diff: L{int}
        @param start: min values of pointers to modify (inclusive)
        @type start: L{int}
        @param end: max values of pointers to modify (exclusive)
        @type end: L{int}
        """
        assert isinstance(start, int) or (start is None)
        assert isinstance(end, int) or (end is None)
        assert (start is None or end is None) or (end >= start)

        did_modify = False

        for i in range(len(self._pointers)):
            pointer = self._pointers[i]
            if (start is not None) and (pointer < start):
                continue
            if (end is not None) and (pointer >= end):
                continue
            self._pointers[i] += diff
            did_modify = True

        # check if this list should be marked as dirty
        if did_modify:
            self.mark_dirty()

    def get_disk_size(self):
        return struct.calcsize(constants.ENDIAN + self.POINTER_FORMAT) * len(self._pointers)


class OrderedPointerList(SimplePointerList):
    """
    An ordered pointer list of a ZIM file.

    This is used by the URL pointer list and title pointer list.

    An ordered pointer list is used to map ordered entries to their
    locations. The order is determined by an external attribute (e.g.
    the URLs of the entry pointed at). The pointer list itself only
    contains the pointers, finding a pointer for a key requires the
    loading of the entries via the pointers.

    @ivar _keyf: a function that returns the bytestring by which this pointer list is sorted
    @type _keyf: a callable returning L{bytes}
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
    def new(cls, key_func):
        """
        Instantiate a new, empty pointerlist.

        @param key_func: a function that returns the bytestring by which this pointer list is sorted.
        @type key_func: a callable returning L{bytes}
        @return: a new, empty pointerlist
        @rtype: L{OrderedPointerList}
        """
        return cls([], key_func=key_func)

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

    @classmethod
    def from_zim_file(cls, zim, n, key_func, seek=None):
        """
        Load a pointer list from a ZIM file at the specified offset.

        This usually acquires the file lock and calls L{SimplePointerList.from_file},
        but subclasses may behave differently.

        @param zim: ZIM file to read from
        @type zim: L{pyzim.archive.Zim}
        @param n: number of entries in the pointer list.
        @type n: L{int}
        @param key_func: a function that returns the bytestring by which this pointer list is sorted.
        @type key_func: a callable returning L{bytes}
        @param seek: if specified, seek this position of the file before reading
        @type seek: L{int} or L{None}
        @return: the pointerlist read from the file
        @rtype: L{pyzim.pointerlist.SimplePointerList}
        """
        assert isinstance(n, int) and (n >= 0)
        assert isinstance(seek, int) or (seek is None)
        with zim.acquire_file() as f:
            return cls.from_file(f=f, n=n, key_func=key_func, seek=seek)

    @classmethod
    def from_zim_entry(cls, zim, full_url, key_func):
        """
        Load a pointer list from an entry inside a ZIM archive.

        NOTE: it is not possible to validate that the string contains the whole pointer list.

        @param zim: ZIM file to read from
        @type zim: L{pyzim.archive.Zim}
        @param full_url: full url of entry to read pointerlist from
        @type full_url: L{str}
        @param key_func: a function that returns the bytestring by which this pointer list is sorted.
        @type key_func: a callable returning L{bytes}
        @return: the pointerlist parsed from the bytes
        @rtype: L{pyzim.pointerlist.SimplePointerList}
        """
        entry = zim.get_entry_by_full_url(full_url).resolve()
        return cls.from_bytes(entry.read(), key_func=key_func)

    def get(self, key):
        """
        Return the pointer for the specified key.

        The key must be in the same format as returned by the keyfunc.

        @param key: key to search for
        @type key: L{str} or L{bytes}
        @return: the pointer matching the key
        @rtype: L{int}
        @raises KeyError: when no matching key was found.
        """
        assert isinstance(key, (str, bytes))
        return self.get_by_index(self.get_index(key))

    def get_index(self, key):
        """
        Return the index of the pointer for the specified key.

        The key must be in the same format as returned by the keyfunc.

        @param key: key to search for
        @type key: L{str} or L{bytes}
        @return: the index of the pointer matching the key
        @rtype: L{int}
        @raises KeyError: when no matching key was found.
        """
        assert isinstance(key, (str, bytes))
        if isinstance(key, str):
            key = key.encode(constants.ENCODING)
        i = binarysearch(self, key, key=self._keyf)
        if i != len(self) and self._keyf(self.get_by_index(i)) == key:
            return i
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
        @return: the new index in the pointer list
        @rtype: L{int}
        @raises pyzim.exceptions.NonMutable: if pointer list is not mutable
        """
        assert isinstance(key, (str, bytes))
        assert isinstance(pointer, int) and (pointer >= 0)
        if isinstance(key, str):
            key = key.encode(constants.ENCODING)
        self.ensure_mutable()
        with self._lock:
            i = binarysearch(self, key, key=self._keyf)
            self._pointers.insert(i, pointer)
            self.mark_dirty()
        return i

    def remove(self, key):
        """
        Remove a pointer from this pointer list.

        @param key: key of pointer to remove
        @type key: L{str} or L{bytes}
        @raises KeyError: when no matching key was found.
        @raises pyzim.exceptions.NonMutable: if pointer list is not mutable
        """
        assert isinstance(key, (bytes, str))
        if isinstance(key, str):
            key = key.encode(constants.ENCODING)
        self.ensure_mutable()
        with self._lock:
            i = binarysearch(self, key, key=self._keyf)
            if i != len(self._pointers) and self._keyf(self._pointers[i]) == key:
                del self._pointers[i]
                self.mark_dirty()
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
        return binarysearch(self, key, key=self._keyf)

    def iter_values(self, start=None, end=None):
        """
        Iterate over all values referenced by the pointers in this pointer list.

        If start and end are specified, they reference the indexes of the
        first (inclusive) and last (exclusive) pointers to return the
        value of. In other words, this behavior matches the l[start:end]
        syntax.

        @param start: index of first pointer to return value of (inclusive)
        @type start: L{int}
        @param end: index of last pointer to return value of (exclusive)
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
            yield self._keyf(self.get_by_index(i))

    def check_sorted(self):
        """
        Check that this list is actually ordered correctly.

        @raises pyzim.exceptions.UnsortedList: when the pointers are not correctly ordered.
        """
        last_key = None
        for pointer in self.iter_pointers():
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

    def print_content(self):  # pragma: no cover
        """
        Print the content of this pointerlist, including the keys.
        """
        for i in range(len(self)):
            pointer = self.get_by_index(i)
            print("{} -> {}: {}".format(i, pointer, repr(self._keyf(pointer))))


class TitlePointerList(OrderedPointerList):
    """
    A pointer list used by the ZIM title listings.

    Unlike other pointer lists, these pointers do not refer to offsets
    but to entry IDs.
    """
    POINTER_FORMAT = "I"


# ============ ON-DISK VARIANTS =============


class OnDiskSimplePointerList(SimplePointerList):
    """
    A variant of L{SimplePointerList} that reads pointers always from the disk.

    This should be more RAM efficient, but beware that it is likely quite slow.

    @ivar _zim: zim this pointer list is part of
    @type _zim: L{pyzim.archive.Zim}
    @ivar _offset: offset of this pointerlist in the zim file
    @type _offset: L{int}
    @ivar _n: amount of entries in this pointerlist
    @type _n: L{int}
    @ivar _item_size: the size of each item in this list in bytes
    @type _item_size: L{int}
    """
    def __init__(self, zim, offset, n):
        """
        The default constructor.

        @param zim: zim this pointer list is part of
        @type zim: L{pyzim.archive.Zim}
        @param offset: offset of this pointerlist in the zim file
        @type offset: L{int}
        @param n: amount of entries in this pointerlist
        @type n: L{int}
        """
        SimplePointerList.__init__(self, [])
        self._zim = zim
        self._offset = offset
        self._n = n
        self._item_size = struct.calcsize(constants.ENDIAN + self.POINTER_FORMAT)
        self.mutable = False

    @classmethod
    def new(cls):
        raise exceptions.OperationNotSupported("Can not create a new, empty {c}, only load an existing one.".format(c=cls.__name__))

    @classmethod
    def from_bytes(cls, s):
        raise exceptions.OperationNotSupported("{c} can not be instantiated from bytes, try to use {c}.from_zim_file() instead!".format(c=cls.__name__))

    @classmethod
    def from_file(cls, f, n, seek=None):
        raise exceptions.OperationNotSupported("{c} can not be instantiated directly from a file, try to use {c}.from_zim_file() instead!".format(c=cls.__name__))

    @classmethod
    def from_zim_file(cls, zim, n, seek=None):
        assert isinstance(n, int) and (n >= 0)
        assert isinstance(seek, int) or (seek is None)
        if seek is None:
            with zim.acquire_file() as f:
                seek = f.tell()
        return cls(zim, n=n, offset=seek)

    @classmethod
    def from_zim_entry(cls, zim, full_url):
        entry = zim.get_entry_by_full_url(full_url).resolve()
        size = entry.get_size()
        n = (size // struct.calcsize(constants.ENDIAN + cls.POINTER_FORMAT))
        cluster = entry.get_cluster()
        offset = cluster.offset + 1 + cluster.get_offset(entry.blob_number)
        return cls.from_zim_file(zim, n, seek=offset)

    def __len__(self):
        return self._n

    def get_by_index(self, i):
        assert isinstance(i, int)
        if (i >= self._n) or (i < 0):
            raise IndexError("Index {} is outside of pointer list!".format(i))
        full_offset = self._offset + (i * self._item_size)
        with self._zim.acquire_file() as f:
            f.seek(full_offset)
            data = f.read(self._item_size)
        pointer = struct.unpack(constants.ENDIAN + self.POINTER_FORMAT, data)[0]
        return pointer


class OnDiskOrderedPointerList(OnDiskSimplePointerList, OrderedPointerList):

    def __init__(self, zim, offset, n, key_func):
        """
        The default constructor.

        @param zim: zim this pointer list is part of
        @type zim: L{pyzim.archive.Zim}
        @param offset: offset of this pointerlist in the zim file
        @type offset: L{int}
        @param n: amount of entries in this pointerlist
        @type n: L{int}
        @param key_func: a function that returns the bytestring by which this pointer list is sorted.
        @type key_func: a callable returning L{bytes}
        """
        OrderedPointerList.__init__(self, pointers=[], key_func=key_func)
        OnDiskSimplePointerList.__init__(self, zim=zim, offset=offset, n=n)

    @classmethod
    def new(cls, key_func):
        raise exceptions.OperationNotSupported("Can not create a new, empty {c}, only load an existing one.".format(c=cls.__name__))

    @classmethod
    def from_bytes(cls, s, key_func):
        raise exceptions.OperationNotSupported("{c} can not be instantiated from bytes, try to use {c}.from_zim_file() instead!".format(c=cls.__name__))

    @classmethod
    def from_file(cls, f, n, key_func, seek=None):
        raise exceptions.OperationNotSupported("{c} can not be instantiated from a file, try to use {c}.from_zim_file() instead!".format(c=cls.__name__))

    @classmethod
    def from_zim_file(cls, zim, n, key_func, seek=None):
        assert isinstance(n, int) and (n >= 0)
        assert isinstance(seek, int) or (seek is None)
        if seek is None:
            with zim.acquire_file() as f:
                seek = f.tell()
        return cls(zim=zim, offset=seek, n=n, key_func=key_func)

    @classmethod
    def from_zim_entry(cls, zim, full_url, key_func):
        entry = zim.get_entry_by_full_url(full_url).resolve()
        size = entry.get_size()
        n = (size // struct.calcsize(constants.ENDIAN + cls.POINTER_FORMAT))
        cluster = entry.get_cluster()
        offset = cluster.offset + 1 + cluster.get_offset(entry.blob_number)
        return cls.from_zim_file(zim, n, seek=offset, key_func=key_func)


class OnDiskTitlePointerList(TitlePointerList, OnDiskOrderedPointerList):
    """
    A on-disk pointer list used by the ZIM title listings.

    See L{TitlePointerList} and L{OnDiskSimplePointerList} for more info.
    """
    pass
