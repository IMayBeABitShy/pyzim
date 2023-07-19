"""
This module contains the top-level archive object, which provides the main API.
"""
import threading
import contextlib
import hashlib

from . import constants
from .exceptions import ZimFileClosed, EntryNotFound
from .header import Header
from .mimetypelist import MimeTypeList
from .pointerlist import SimplePointerList, OrderedPointerList, TitlePointerList
from .entry import BaseEntry
from .policy import Policy, DEFAULT_POLICY
from .ioutil import read_n_bytes


class Zim(object):
    """
    A ZIM archive.

    @ivar header: header of this ZIM file.
    @type header: L{pyzim.header.Header}
    @ivar filelock: a lock to ensure file access works with multiple threads. Acquire if whenever any work is done on the file.
    @type filelock: L{threading.Lock}
    @ivar mimetypelist: the mimetype list
    @type mimetypelist: L{pyzim.mimetypelist.MimeTypeList}
    @ivar policy: policy to use
    @type policy: L{pyzim.policy.Policy}
    @ivar cluster_cache: internal cache for clusters, mapping the full location to each cluster
    @type cluster_cache: L{pyzim.cache.BaseCache}
    @ivar entry_cache: internal cache for entries, mapping the full location to each cluster
    @type entry_cache: L{pyzim.cache.BaseCache}
    """
    def __init__(self, f, offset=0, policy=DEFAULT_POLICY):
        """
        The default constructor for opening a ZIM file.

        @param f: file-like object to read from
        @type f: file-like object
        @param offset: offset of the ZIM archive within the file.
        @type offset: L{int}
        @param policy: policy to use, default to L{pyzim.policy.DEFAULT_POLICY}
        @type policy: L{pyzim.policy.Policy}
        """
        assert isinstance(offset, int) and offset >= 0
        assert isinstance(policy, Policy)
        self._f = f
        self._base_offset = offset
        self.policy = policy
        self._closed = False
        self.filelock = threading.Lock()

        self._init_caches()
        self._load_header()
        self._load_mimetypelist()
        self._load_pointerlists()

    @classmethod
    def open(cls, path, mode="r", policy=DEFAULT_POLICY):
        """
        Open the Zim archive at the specified path.

        @param path: path to open
        @type path: L{str}
        @param mode: mode of the Zim archive (currently, only reading is supported)
        @type mode: L{str}
        @return: the Zim archive opened from the file
        @rtype: L{pyzim.archive.Zim}
        @param policy: policy to use, default to L{pyzim.policy.DEFAULT_POLICY}
        @type policy: L{pyzim.policy.Policy}
        """
        if mode != "r":
            raise NotImplementedError("Currently, only reading mode is implemented!")
        f = open(path, "rb")
        archive = cls(f, policy=policy)
        return archive

    def _init_caches(self):
        """
        Initializes internal caches according to policy.
        """
        self.cluster_cache = self.policy.cluster_cache_class(
            **self.policy.cluster_cache_kwargs,
        )
        self.entry_cache = self.policy.entry_cache_class(
            **self.policy.entry_cache_kwargs,
        )

    def _load_header(self):
        """
        Read the header.
        """
        with self.filelock:
            # read header/meta data from file
            self.header = Header.from_file(self._f, seek=self._base_offset)

    def _load_mimetypelist(self):
        """
        Load the mimetypelist.
        """
        assert self.header is not None
        pos = self._base_offset + self.header.mime_list_position
        with self.filelock:
            self.mimetypelist = MimeTypeList.from_file(self._f, seek=pos)

    def _load_pointerlists(self):
        """
        Load the URL and title pointer lists.
        """
        assert self.header is not None
        url_pointer_position = self._base_offset + self.header.url_pointer_position
        cluster_pointer_position = self._base_offset + self.header.cluster_pointer_position
        with self.filelock:
            # the URL pointer list
            self._url_pointer_list = OrderedPointerList.from_file(
                self._f,
                self.header.entry_count,
                key_func=self._get_full_url_for_entry_at,
                seek=url_pointer_position,
            )
            # the cluster pointer list
            self._cluster_pointer_list = SimplePointerList.from_file(
                self._f,
                self.header.cluster_count,
                seek=cluster_pointer_position,
            )
        # releasing lock, as we are accessing these via higher level APIs
        # the entry title pointer list
        # this may either be part of the header or accessed
        # via an URL
        if self.has_entry_for_full_url(constants.URL_ENTRY_TITLE_INDEX):
            self._entry_title_pointer_list = TitlePointerList.from_bytes(
                self.get_entry_by_full_url(constants.URL_ENTRY_TITLE_INDEX).read(),
                key_func=self._get_namespace_title_for_entry_at,
            )
        else:
            # unfortunately, we need to fall back to the header title pointer position
            # this requires us to re-acquire the file lock
            with self.filelock:
                self._f.seek(self.header.title_pointer_position)
                entry_title_pointer_data = read_n_bytes(
                    self._f,
                    4 * self.header.entry_count,
                    raise_on_incomplete=True
                )
            self._entry_title_pointer_list = TitlePointerList.from_bytes(
                entry_title_pointer_data,
                key_func=self._get_namespace_title_for_entry_by_url_index,
            )
        # the article title pointer list
        self._article_title_pointer_list = TitlePointerList.from_bytes(
            self.get_entry_by_full_url(constants.URL_ARTICLE_TITLE_INDEX).read(),
            key_func=self._get_title_for_entry_by_url_index,
        )

    def _get_full_url_for_entry_at(self, location):
        """
        Return the full URL for the entry with at the specified location.

        This is used as the key function for the URL pointer list.

        @param location: location of the entry in the ZIM file
        @type location: L{int}
        @return: the full URL of the specified entry
        @rtype: L{bytes}
        """
        entry = self.get_entry_at(location)
        return entry.full_url.encode(constants.ENCODING)

    def _get_namespace_title_for_entry_by_url_index(self, i):
        """
        Return the namespace+title for the entry at the specified index in the URL pointer list.

        This is used as the key function for the entry title pointer list.

        @param i: index of the entry in the URL pointer list
        @type i: L{int}
        @return: the <namespace><title> of the entry
        @rtype: L{str}
        """
        entry = self.get_entry_by_url_index(i)
        return (entry.namespace + entry.title).encode(constants.ENCODING)

    def _get_title_for_entry_by_url_index(self, i):
        """
        Return the title for the entry at the specified index in the URL pointer list.

        This is used as the key function for the article pointer list.

        @param i: index of the entry in the URL pointer list
        @type i: L{int}
        @return: the title of the specified entry
        @rtype: L{str}
        """
        entry = self.get_entry_by_url_index(i)
        return entry.title.encode(constants.ENCODING)

    def _check_closed(self):
        """
        Check to ensure this ZIM file has not already been closed.

        @raises pyzim.exceptions.ZimFileClosed: when the ZIM file is already closed.
        """
        if self._closed:
            raise ZimFileClosed("ZIM file already closed!")

    def close(self):
        """
        Close the ZIM file. Can be safely called multiple times.
        """
        self._f.close()
        self._closed = True

    @property
    def closed(self):
        """
        Return True if this archive has already been closed, False otherwise.

        @return: whether this archive has already been closed or not
        @rtype: L{bool}
        """
        return self._closed

    def __enter__(self):
        """
        Called upon entering a with-statement. Provides self as object for the context.
        """
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        if not self.closed:
            self.close()
        return False

    @contextlib.contextmanager
    def acquire_file(self):
        """
        A context manager that locks the file access and provides the wrapped file object for the context.
        """
        self._check_closed()
        with self.filelock:
            yield self._f

    # ============= entry functions ==========

    def get_entry_at(self, location, bind=True):
        """
        Return the entry at the specified location (offset) in the ZIM file.

        @param location: location/offset of the entry in the ZIM file
        @type location: L{int}
        @param bind: if nonzero (default), bind this entry
        @type bind: L{bool}
        @return: the entry at the specified location
        @rtype: L{pyzim.entry.BaseEntry}
        """
        assert isinstance(location, int) and location >= 0
        full_location = self._base_offset + location
        if self.entry_cache.has(full_location):
            entry = self.entry_cache.get(full_location)
        else:
            with self.filelock:
                entry = BaseEntry.from_file(self._f, seek=full_location)
            self.entry_cache.push(full_location, entry)
        if bind:
            entry.bind(self)
        return entry

    def get_entry_by_url(self, namespace, url):
        """
        Return the entry at the specified (non-full) URL.

        @param namespace: namespace of entry to get
        @type namespace: L{str} of length 1
        @param url: url of entry to get
        @type url: L{str}
        @return: the entry at the specified url
        @rtype: L{pyzim.entry.BaseEntry}
        @raises pyzim.exceptions.EntryNotFound: when no entry matches the specified URL
        """
        assert isinstance(namespace, str) and len(namespace) == 1
        assert isinstance(url, str)
        full_url = namespace + url
        return self.get_entry_by_full_url(full_url)

    def get_content_entry_by_url(self, url):
        """
        Return the entry at the specified (non-full) URL in the "C" namespace.

        NOTE: "content" refers to an entry in the "C" namespace. This
        function may still return any type of L{pyzim.entry.BaseEntry}
        and is NOT restricted to L{pyzim.entry.ContentEntry}.

        @param url: url of entry to get
        @type url: L{str}
        @return: the entry at the specified url
        @rtype: L{pyzim.entry.BaseEntry}
        @raises pyzim.exceptions.EntryNotFound: when no entry matches the specified URL
        """
        assert isinstance(url, str)
        return self.get_entry_by_url("C", url)

    def get_entry_by_full_url(self, full_url):
        """
        Return the entry at the specified full URL.

        @param full_url: full URL of entry to get
        @type full_url: L{str}
        @return: the entry at the specified URL
        @rtype: L{pyzim.entry.BaseEntry}
        @raises pyzim.exceptions.EntryNotFound: when no entry matches the specified URL
        """
        assert isinstance(full_url, str)
        try:
            location = self._url_pointer_list.get(full_url)
        except KeyError:
            raise EntryNotFound("No entry for full URL '{}'".format(full_url))
        return self.get_entry_at(location)

    def get_entry_by_url_index(self, i):
        """
        Return the entry at the specified index in the URL pointer list.

        @param i: index of entry in URL pointer list
        @type i: L{int}
        @return: the entry at the specified location
        @rtype: L{pyzim.entry.BaseEntry}
        @raises pyzim.exceptions.EntryNotFound: when no entry matching the index was found
        """
        assert isinstance(i, int)
        if i < 0:
            # disallow negative indexes
            # while index -1 is valid in python, most times this function
            # will receive a negative index is by error, not by design
            raise EntryNotFound("Index {} is negative, suspected as error".format(i))
        try:
            location = self._url_pointer_list.get_by_index(i)
        except IndexError:
            raise EntryNotFound("No entry for URL pointer list index {}".format(i))
        return self.get_entry_at(location)

    def has_entry_for_full_url(self, full_url):
        """
        Return True if this ZIM file contains an entry for the specified full URL.

        @param full_url: full URL of entry to check existence of
        @type full_url: L{str}
        @return: True if an entry for this full URL exists. It may be a redirect.
        @rtype: L{bool}
        """
        assert isinstance(full_url, str)
        return self._url_pointer_list.has(full_url)

    def get_mainpage_entry(self):
        """
        Return the entry for the mainpage.

        @return: the entry for the mainpage
        @rtype: L{pyzim.entry.BaseEntry}
        @raises pyzim.exceptions.EntryNotFound: when no mainpage exists
        """
        if not self.header.has_main_page:
            raise EntryNotFound("This ZIM file does not contain a main page!")
        main_page_index = self.header.main_page
        entry_location = self._url_pointer_list.get_by_index(main_page_index)
        entry = self.get_entry_at(entry_location)
        return entry

    def iter_entries(self, start=None, end=None):
        """
        Iterate over all entries in this ZIM.

        If start and end are specified, they reference the indexes of the
        first (inclusive) and last (exclusive) entry to return.
        In other words, this behavior matches the l[start:end] syntax.

        This function does not guarantee any specific order of the entries
        yielded by this function, however it currently *should* be ordered
        by namespace and title.

        @param start: index of first entry to return (inclusive)
        @type start: L{int}
        @param end: index of last entry to return (exclusive)
        @type end: L{int}
        @yield: the entries in the specified range
        @ytype: L{pyzim.entry.BaseEntry}
        """
        for i in self._entry_title_pointer_list.iter_pointers(start=start, end=end):
            yield self.get_entry_by_url_index(i)

    def iter_articles(self, start=None, end=None):
        """
        Iterate over all article entries in this ZIM.

        If start and end are specified, they reference the indexes of the
        first (inclusive) and last (exclusive) entry to return.
        In other words, this behavior matches the l[start:end] syntax.

        This function does not guarantee any specific order of the entries
        yielded by this function, however it currently *should* be ordered
        by title.

        @param start: index of first entry to return (inclusive)
        @type start: L{int}
        @param end: index of last entry to return (exclusive)
        @type end: L{int}
        @yield: the entries in the specified range
        @ytype: L{pyzim.entry.BaseEntry}
        """
        for i in self._article_title_pointer_list.iter_pointers(start=start, end=end):
            yield self.get_entry_by_url_index(i)

    # ========== mimetype interface ==========

    def get_mimetype_by_index(self, i):
        """
        Return the mimetype with the specified index.

        @param i: index of mimetype to get
        @type i: L{int}
        @return: the mimetype with the specified index
        @rtype: L{str}
        @raises IndexError: when the index is invalid
        """
        assert isinstance(i, int)
        return self.mimetypelist.get(i, as_unicode=True)

    def get_mimetype_of_entry(self, entry):
        """
        Return the mimetype of the specified entry.

        If the entry is a redirect, this will be L{pyzim.constants.MIMETYPE_REDIRECT}.

        @param entry: entry to get mimetype for
        @type entry: L{pyzim.entry.BaseEntry}
        @return: the mimetype of this entry
        @rtype: L{str}
        """
        assert isinstance(entry, BaseEntry)
        if entry.is_redirect:
            return constants.MIMETYPE_REDIRECT
        return self.get_mimetype_by_index(entry.mimetype_id)

    def iter_mimetypes(self, as_unicode=False):
        """
        Iterate over all mimetypes in this archive.

        @param as_unicode: if nonzero, decode mimetypes
        @type as_unicode: L{bool}
        @yields: the mimetypes in this mimetype list
        @ytype: L{bytes} or L{str} if as_unicode is nonzero
        """
        yield from self.mimetypelist.iter_mimetypes(as_unicode=as_unicode)

    # =========== cluster interface ===========

    def get_cluster_at(self, location):
        """
        Return the cluster at the specified location (offset) in the ZIM file.

        @param location: location/offset of the cluster in the ZIM file
        @type location: L{int}
        @return: the entry at the specified location
        @rtype: L{pyzim.cluster.Cluster}
        """
        # not offering bind as a parameter as that would make this function useless
        full_location = self._base_offset + location
        if self.cluster_cache.has(full_location):
            return self.cluster_cache.get(full_location)
        else:
            cluster_class = self.policy.cluster_class
            cluster = cluster_class(zim=self, offset=full_location)
            self.cluster_cache.push(full_location, cluster)
            return cluster

    def get_cluster_by_index(self, i):
        """
        Return the cluster for the specified index.

        @param i: index of cluster to get
        @type i: L{int}
        """
        assert isinstance(i, int) and i >= 0
        pos = self._cluster_pointer_list.get_by_index(i)
        return self.get_cluster_at(pos)

    def iter_clusters(self, start=None, end=None):
        """
        Iterate over all clusters in this ZIM.

        If start and end are specified, they reference the indexes of the
        first (inclusive) and last (exclusive) clusters to return.
        In other words, this behavior matches the l[start:end] syntax.

        @param start: index of first cluster to return (inclusive)
        @type start: L{int}
        @param end: index of last cluster to return (exclusive)
        @type end: L{int}
        @yield: the clusters in the specified range
        @ytype: L{pyzim.cluster.Cluster}
        @raises IndexError: on invalid/out of bound indexes
        """
        if start is None:
            start = 0
        if end is None:
            end = self.header.cluster_count
        assert isinstance(start, int)
        assert isinstance(end, int)
        if (start < 0) or (end > self.header.cluster_count):
            raise IndexError("Start {} or end {} out of range of clusters".format(start, end))
        elif start > end:
            raise IndexError("Start index {} is after end index {}".format(start, end))

        for i in range(start, end):
            yield self.get_cluster_by_index(i)

    def get_cluster_index_by_offset(self, offset):
        """
        Return the cluster index for the cluster at the specified offset.

        Note that the offset must match exactly the offset of the cluster.

        This method is mostly used as a helper by clusters to determine
        their own index.

        @return: the index of the cluster at the offset in the cluser pointer list
        @rtype: L{int}
        @raises KeyError: if the offset does not refer to a cluster.
        """
        for i, ptr_offset in enumerate(self._cluster_pointer_list.iter_pointers()):
            if ptr_offset == offset:
                return i
        raise KeyError("No cluster at offset {}!".format(offset))

    # =============== metadata interface ==========

    def get_metadata(self, key, as_unicode=True):
        """
        Read a metadata entry, returning its value.

        See https://wiki.openzim.org/wiki/Metadata for metadata keys and
        values.

        By default, this method returns unicode. You can set as_unicode=False
        to prevent this. If the key is not found, return L{None}.

        @param key: key/URL of metadata
        @type key: L{str}
        @param as_unicode: whether to decode value or not
        @type as_unicode: L{bool}
        @return: the metadata value
        @rtype: L{str} or L{bytes} (or L{None} if not found)
        """
        try:
            entry = self.get_entry_by_url("M", key).resolve()
        except EntryNotFound:
            # no such key defined
            return None
        content = entry.read()
        if as_unicode:
            return content.decode(constants.ENCODING)
        return content

    def get_metadata_keys(self, as_unicode=True):
        """
        Read all metadata keys, returning them as a list.

        By default, this method returns unicode. You can set as_unicode=False
        to prevent this. If the key is not found, return L{None}.

        @param as_unicode: whether to decode value or not
        @type as_unicode: L{bool}
        @return: the metadata keys
        @rtype:L{list} of L{str} or L{bytes}
        """
        # all metadata keys are URLs in the "M" namespace
        # we search for all entries in this namespace
        keys = []
        start_i = self._url_pointer_list.find_first_greater_equals("M")
        for entry in self.iter_entries(start=start_i):
            if entry.namespace != "M":
                # end of namespace
                break
            url = entry.url
            if not as_unicode:
                # encode again
                # TODO: inefficient, perhaps make .url a property and
                # expose the raw_url value
                url = url.encode(constants.ENCODING)
            keys.append(url)
        return keys

    def get_metadata_dict(self, as_unicode=True):
        """
        Return a dict containing all metadata of this ZIM.

        NOTE: values of certain metadata keys won't be decoded.
        This prevents the decoding of binary content of images..

        @param as_unicode: whether to decode strings or not
        @type as_unicode: L{bool}
        @return: a dict containing the metadata
        @rtype: L{dict} of L{str} or L{bytes} -> L{bytes} or L{str}
        """
        # TODO: currently, we are accessing each entry twice, fix this
        keys = self.get_metadata_keys(as_unicode=as_unicode)
        metadata = {}
        for key in keys:
            unicode_key = (key.decode(constants.ENCODING) if isinstance(key, bytes) else key)
            do_decode = as_unicode and not unicode_key.startswith("Illustration_")
            metadata[key] = self.get_metadata(unicode_key, as_unicode=do_decode)
        return metadata

    # TODO: illustration support -> PIL/Pillow

    # =========== checksum functions ==============

    def get_checksum(self):
        """
        Read the checksum of this ZIM file and return it.

        NOTE: this reads the checksum from the ZIM file, it does not
        calculate the actual checksum of the file.
        If you want to calculate the checksum of the ZIM, use
        L{pyzim.archive.Zim.calculate_checksum} instead.

        @return: the (md5) checksum of this ZIM
        @rtype: L{bytes}
        """
        with self.acquire_file() as f:
            f.seek(self.header.checksum_position)
            checksum = f.read(constants.CHECKSUM_LENGTH)
        return checksum

    def calculate_checksum(self):
        """
        Calculate the checksum of this ZIM file and return it.

        NOTE: this reads the entire ZIM file and calculates the ZIM file.
        If you want to read the checksum listed in the ZIM file, use
        L{pyzim.archive.Zim.get_checksum} instead.

        @return: the calculated (md5) checksum of this ZIM
        @rtype: L{bytes}
        """
        hasher = hashlib.md5()
        end = self.header.checksum_position
        chunksize = 8192
        with self.acquire_file() as f:
            f.seek(0)
            pos = 0
            while pos < end:
                to_read = min(chunksize, end - pos)
                data = f.read(to_read)
                pos += len(data)
                hasher.update(data)
        return hasher.digest()


# Fix for pydoctor, which would otherwise hide all names exported in __init__.__all__
__all__ = ["Zim"]
