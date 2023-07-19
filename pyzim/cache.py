"""
Various cache strategies.

Caches are responsible for ensuring that access to entries and clusters
is as fast as possible. This module offers mulitple cache implementations
which are meant to reduce the amount of times data has to actually be
uncompressed from the cluster.
"""


class BaseCache(object):
    """
    Base class for caches.

    When using any of the subclasses of this cache, please use something
    akin to the following pattern::

        key = ...  # some unique hashable key identifying the element
        if cache.has(key):  # cache hit
            element = cache.get(key)
        else:
            element = ...  # get element outside of cache
            cache.push(key)  # always push element unto the cache

    This class does not actually implement anything and serves as a
    superclass that describes the methods.
    """
    def has(self, key):
        """
        Return True if an element is cached for the specific key.

        @param key: key of element that should be checked
        @type key: hashable
        @return: True if an element has been cached for this key
        @rtype: L{bool}
        """
        raise NotImplementedError("BaseCache.has() needs to be overwritten in subclasses!")

    def get(self, key):
        """
        Return the element that has been cached for this key.

        @param key: key for the element that should be returned
        @type key: hashable
        @return: the element
        @rtype: same as passed to L{BaseCache.push}
        @raises KeyError: if no element was cached for this key
        """
        raise NotImplementedError("BaseCache.get() needs to be overwritten in subclasses!")

    def push(self, key, element):
        """
        Push an element for this key into this cache.

        It is up for the cache to decice whether the element will
        actually be cached or not.

        Re-pushing an already cached element will not update the cached
        element.

        @param key: key which will be used to identify the element
        @type key: hashable
        @param element: element to cache
        @type element: any
        @return: True if the element is now cached, False otherwise
        @rtype: L{bool}
        """
        raise NotImplementedError("BaseCache.push() needs to be overwritten in subclasses!")

    def clear(self):
        """
        Empty this cache.
        """
        raise NotImplementedError("BaseCache.clear() needs to be overwritten in subclasses!")


class NoOpCache(BaseCache):
    """
    A L{BaseCache} implementation that does not cache.
    """
    def has(self, key):
        return False

    def get(self, key):
        raise KeyError("No element cached for key {}!".format(repr(key)))

    def push(self, key, element):
        return False

    def clear(self):
        pass


class _DoubleLinkedListElement(object):
    """
    An element in a double linked list used by the LRU cache.

    @ivar parent: DoubleLinkedList this element is a part of
    @type parent: L{_DoubleLinkedList}
    @ivar value: value of this element
    @type value: any
    @ivar next: next element in this double linked list
    @type next: L{_DoubleLinkedListElement} or L{None}
    @ivar prev: previous element in this double linked list
    @type prev: L{_DoubleLinkedListElement} or L{None}
    """
    def __init__(self, parent, value):
        """
        The default constructor.

        @param parent: DoubleLinkedList this element is a part of
        @type parent: L{_DoubleLinkedList}
        @param value: value of this double linked list element
        @type value: any
        """
        assert isinstance(parent, _DoubleLinkedList)
        self.parent = parent
        self.value = value
        self.next = None
        self.prev = None

    def __repr__(self):
        """
        Return a string representing this object

        @return: a string representing this object
        @rtype: L{str}
        """
        return "{}(value={})".format(self.__class__.__name__, repr(self.value))

    def swap_forward(self):
        """
        Swap this element once forward towards the head.

        If this is already the head, nothing happens.
        """
        if self.prev is not None:
            old_prev = self.prev
            self.prev = old_prev.prev
            old_prev.next = self.next
            if self.next is not None:
                self.next.prev = old_prev
            self.next = old_prev
            if old_prev.prev is not None:
                old_prev.prev.next = self
            old_prev.prev = self

            if self.prev is None:
                # new head
                self.parent.head = self
            if old_prev.next is None:
                # new tail
                self.parent.tail = old_prev


class _DoubleLinkedList(object):
    """
    A double linkked list used by the LRU cache.

    @ivar head: the first element in this list
    @type head: L{_DoubleLinkedListElement} or L{None}
    @ivar tail: the last element in this list
    @type tail: L{_DoubleLinkedListElement} or L{None}
    @ivar length: current length of list
    @type length: L{int}
    """
    def __init__(self, elements=None):
        """
        The default constructor.

        @param elements: if specified, add all elements from this iterable to this list
        @type elements: iterator or L{None}
        """
        self.head = None
        self.tail = None
        self.length = 0
        if elements is not None:
            for element in elements:
                self.append(element)

    def __iter__(self):
        """
        Iterate over the values in this list.

        @yield: the values in this list
        @ytype: same as added to this list
        """
        yield from self.iter_values()

    def __repr__(self):
        """
        Return a string representing this object

        @return: a string representing this object
        @rtype: L{str}
        """
        return "{}({})".format(self.__class__.__name__, repr(self.to_list()))

    def to_list(self):
        """
        Return a python list of the values in this list.

        Complexity is O(n).

        @return: a list containing the values in this list
        @rtype: L{list}
        """
        return [v for v in self.iter_values()]

    def append(self, value):
        """
        Append an element to the end of this list.

        If value if a L{_DoubleLinkedListElement}, it will not be
        wrapped and instead used directly.

        Complexity is O(1).

        @param value: value or element to append
        @type value: any
        @return: the added list element
        @rtype: L{_DoubleLinkedListElement}
        """
        if isinstance(value, _DoubleLinkedListElement):
            element = value
            element.parent = self
            element.next = element.prev = None
        else:
            element = _DoubleLinkedListElement(self, value)
        if self.tail is not None:
            self.tail.next = element
            element.prev = self.tail
            self.tail = element
            self.length += 1
        else:
            self.head = self.tail = element
            self.length = 1
        return self.tail

    def prepend(self, value):
        """
        Prepend an element at the front of this list.

        If value if a L{_DoubleLinkedListElement}, it will not be
        wrapped and instead used directly.

        Complexity is O(1).

        @param value: value or element to prepend
        @type value: any
        @return: the added list element
        @rtype: L{_DoubleLinkedListElement}
        """
        if isinstance(value, _DoubleLinkedListElement):
            element = value
            element.parent = self
            element.next = element.prev = None
        else:
            element = _DoubleLinkedListElement(self, value)
        if self.head is not None:
            self.head.prev = element
            element.next = self.head
            self.head = element
            self.length += 1
        else:
            self.head = self.tail = element
            self.length = 1
        return self.head

    def clear(self):
        """
        Empty this list.

        Complexity is O(1).
        """
        self.head = self.tail = None
        self.length = 0

    def remove_last(self):
        """
        Remove the last element of this list.

        Complexity is O(1).

        @raises IndexError: if there is no last element
        """
        if self.tail is None:
            raise IndexError("List is empty!")
        new_tail = self.tail.prev
        if new_tail is not None:
            new_tail.next = None
        else:
            # no previous element, current is head
            self.head = None
        self.tail = new_tail
        self.length -= 1

    def remove_first(self):
        """
        Remove the first element of this list.

        Complexity is O(1).

        @raises IndexError: if there is no first element
        """
        if self.head is None:
            raise IndexError("List is empty!")
        new_head = self.head.next
        if new_head is not None:
            new_head.prev = None
        else:
            # no next element, current is tail
            self.tail = None
        self.head = new_head
        self.length -= 1

    def remove_element(self, element):
        """
        Remove a list element (not a value!).

        Complexity is O(1).

        @param element: element to remove
        @type element: L{_DoubleLinkedListElement}
        @raises ValueError: if element is not part of this list
        """
        assert isinstance(element, _DoubleLinkedListElement)
        if element.parent is not self:
            raise ValueError("Element {} is not part of this list!".format(repr(element)))
        if element.prev is not None:
            # there is a previous element
            element.prev.next = element.next
        else:
            # is head, move head to next element
            self.head = element.next
        if element.next is not None:
            # there is a next element
            element.next.prev = element.prev
        else:
            # is tail, move tail to prev element
            self.tail = element.prev
        element.parent = None
        self.length -= 1

    def iter_values(self):
        """
        Iterate over all values.

        @yield: the values in this list
        @ytype: same as added to this list
        """
        cur = self.head
        while cur is not None:
            yield cur.value
            cur = cur.next


class LastAccessCache(BaseCache):
    """
    A L{BaseCache} implementation that caches by last access.

    This cache works using a double-linked list and a dictionary/hashmap.
    Newly accessed elements are added to the front of the list while the
    dictionary maps the key to the list segment, allowing O(1) access.
    When the list is already "full", the last element gets removed. This
    allows all cache operations within O(1).

    @ivar max_size: maximum size of cache in number of elements
    @type max_size: L{int}
    """
    def __init__(self, max_size=8):
        """
        @param max_size: maximum size of cache in number of elements
        @type max_size: L{int}
        """
        assert isinstance(max_size, int) and max_size >= 0
        self.max_size = max_size
        self._list = _DoubleLinkedList()  # list of (key, value)
        self._key2element = {}  # key -> _DoubleLinkedListElement

    def has(self, key):
        return key in self._key2element

    def get(self, key):
        try:
            element = self._key2element[key]
        except KeyError:
            raise KeyError("No element cached for key {}!".format(repr(key)))
        # pull element forward
        self._list.remove_element(element)
        self._list.prepend(element)
        # return only the value
        return element.value[1]

    def push(self, key, element):
        if self.has(key):
            # already part of the cache
            return True
        if self._list.length >= self.max_size:
            # we need to remove an element first
            old_key = self._list.tail.value[0]
            del self._key2element[old_key]
            self._list.remove_last()
        # add new element
        new_element = self._list.prepend((key, element))
        self._key2element[key] = new_element
        return True

    def clear(self):
        self._key2element = {}
        self._list.clear()


class TopAccessCache(BaseCache):
    """
    A L{BaseCache} implementation that caches by most accessed.

    This cache uses a double-linked list and two dictionaries.
    The list serves as the actual cache, keeping track of all elements.
    A dictionary maps the keys to the list segments, allowing O(1) access.
    Whenever get() or push() is called, a value is incremented in a
    dictionary. If the value exceeds the value of the tail and the
    element is not yet cached, the tail is replaced. If the value is
    already cached, the list segment may be pulled forward towards the
    head. Of course, whenever there is still space within the cache it
    will also be added to the cache regardless of access count.

    @ivar max_size: max number of elements to cache
    @type max_size: L{int}
    """
    def __init__(self, max_size=8):
        """
        The default constructor.

        @param max_size: max number of elements to cache
        @type max_size: L{int}
        """
        assert isinstance(max_size, int) and max_size >= 0
        self.max_size = max_size
        self._list = _DoubleLinkedList()  # list of (key, element)
        self._key2le = {}  # key -> list element
        self._num_accesses = {}  # key -> number of accesses

    def _increment_access_counter(self, key):
        """
        Increment the number of times an element has been accessed.

        @param key: key to increment counter for
        @type key: hashable
        @return: the number of accesses after incrementation
        @rtype: L{int}
        """
        if key in self._num_accesses:
            num_accesses = self._num_accesses[key] + 1
            self._num_accesses[key] = num_accesses
        else:
            num_accesses = self._num_accesses[key] = 1

        # check if we need to move it forward in the list
        if key in self._key2le:
            le = self._key2le[key]
            if le.prev is not None:
                # not head
                prev_key = le.prev.value[0]
                if self._num_accesses[prev_key] < num_accesses:
                    # we need to swap forward
                    le.swap_forward()

    def _replace_tail_if_necessary(self, key, element):
        """
        If there is yet no value for the key cached but the access count
        is higher than tail, add this value to the cache.

        @param key: key which is used to identify the element
        @type key: hashable
        @param element: element to cache
        @type element: any
        @return: True if the element is now cached, False otherwise
        @rtype: L{bool}
        """
        num_accesses = self._num_accesses[key]
        if key in self._key2le:
            # already cached
            return True
        if self._list.length < self.max_size:
            # add element to tail
            le = self._list.append((key, element))
            self._key2le[key] = le
            return True
        tail = self._list.tail
        old_key = tail.value[0]
        tail_accesses = self._num_accesses[old_key]
        if tail_accesses < num_accesses:
            # replace tail
            self._list.remove_last()
            del self._key2le[old_key]
            le = self._list.append((key, element))
            self._key2le[key] = le
            return True
        return False

    def has(self, key):
        # do not update access counter here
        # it will be updated on get, so avoid multiple update
        return key in self._key2le

    def get(self, key):
        self._increment_access_counter(key)
        return self._key2le[key].value[1]

    def push(self, key, element):
        self._increment_access_counter(key)
        return self._replace_tail_if_necessary(key, element)

    def clear(self):
        self._list.clear()
        self._key2le = {}
        self._num_accesses = {}


class HybridCache(BaseCache):
    """
    A L{pyzim.cache.BaseCache} that internally utilizes both a
    L{pyzim.cache.LastAccessCache} and a L{pyzim.cache.TopAccessCache}.

    @ivar last_cache: the last-access cache
    @type last_cache: L{pyzim.cache.LastAccessCache}
    @ivar top_cache: the top-access cache
    @type top_cache: L{pyzim.cache.TopAccessCache}
    """
    def __init__(self, last_cache_size=8, top_cache_size=8):
        """
        The default constructor.

        @param last_cache_size: size of the last-access cache
        @type last_cache_size: L{int}
        @param top_cache_size: size of the top-access cache
        @type top_cache_size: L{int}
        """
        self.last_cache = LastAccessCache(last_cache_size)
        self.top_cache = TopAccessCache(last_cache_size)

    def has(self, key):
        # always hit top cache first
        if self.top_cache.has(key):
            return True
        elif self.last_cache.has(key):
            return True
        else:
            return False

    def get(self, key):
        if self.top_cache.has(key):
            return self.top_cache.get(key)
        elif self.last_cache.has(key):
            # we should also push it into the top cache
            value = self.last_cache.get(key)
            self.top_cache.push(key, value)
            return value
        else:
            raise KeyError("No element cached for key {}!".format(repr(key)))

    def push(self, key, element):
        # push into top cache first
        # this way, we can keep the last access cache clean from top
        # accessed elements
        did_cache_top = self.top_cache.push(key, element)
        if did_cache_top:
            return True
        did_cache_last = self.last_cache.push(key, element)
        if did_cache_last:
            return True
        # this should probably never happen as the last access cache
        # will always cache
        return False  # pragma: no cover

    def clear(self):
        self.top_cache.clear()
        self.last_cache.clear()
