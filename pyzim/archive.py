"""
This module contains the top-level archive object, which provides the main API.

@var logger: logger for archive-level messages
@type logger: L{logging.Logger}
"""
import threading
import contextlib
import hashlib
import os
import logging

from . import constants
from .blob import InMemoryBlobSource, EmptyBlobSource
from .cluster import ModifiableClusterWrapper, EmptyCluster
from .exceptions import ZimFileClosed, EntryNotFound, BindingError, ZimWriteException
from .header import Header
from .mimetypelist import MimeTypeList
from .entry import BaseEntry, RedirectEntry
from .policy import Policy, DEFAULT_POLICY
from .modifiable import ModifiableMixIn
from .spaceallocator import SpaceAllocator
from .operationbuffer import OperationBuffer
from .item import Item
from .processor import BaseProcessor
from .counter import Counter


# instantiate a logger
logger = logging.getLogger(__name__)


class Zim(ModifiableMixIn):
    """
    A ZIM archive.

    This object can be used to read, write and/or modify a ZIM file.

    NOTE on modifying ZIM archives: to ensure optimal compression, some
    modifications will not immediately be written. This also means that
    reading previously modified entries may not be immediately effective.
    You can force-write all outstanding changes by calling L{pyzim.archive.Zim.flush}.
    This will be done automatically on ZIM close.

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
    @ivar spaceallocator: an object responsible for managing storage space within the ZIM file, may be L{None} if ZIM is read-only
    @type spaceallocator: L{pyzim.spaceallocator.SpaceAllocator} or L{None}
    @ivar compression_strategy: compression strategy for assigning new items to clusters
    @type compression_strategy: L{pyzim.compressionstrategy.BaseCompressionStrategy} or L{None}
    @ivar uncompressed_compression_strategy: compression strategy for assigning new items to clusters that are explicity uncompressed
    @type uncompressed_compression_strategy: L{pyzim.compressionstrategy.BaseCompressionStrategy} or L{None}

    @ivar _f: the underlying file object
    @type _f: file-like object
    @ivar _mode: the mode this archive has been opened in
    @type _mode: L{str}
    @ivar _closed: a flag indicating whether this archive has already been closed
    @type _closed: L{bool}
    @ivar _writable: a flag indicating whether this archvie can be written to.
    @type _writable: L{bool}
    @ivar _base_offset: base offset of ZIM archive within the underlying file object
    @type _base_offset: L{int}
    @ivar _url_pointer_list: a pointer list to entries ordered by URL
    @type _url_pointer_list: L{pyzim.pointerlist.OrderedPointerList}
    @ivar _cluster_pointer_list: a pointer list to the individual clusters
    @type _cluster_pointer_list: L{pyzim.pointerlist.SimplePointerList}
    @ivar _entry_title_pointer_list: a pointerlist to entries ordered by title
    @type _entry_title_pointer_list: L{pyzim.pointerlist.TitlePointerList}
    @ivar _article_title_pointer_list: a pointerlist to article entries ordered by title
    @type _article_title_pointer_list: L{pyzim.pointerlist.TitlePointerList}
    @ivar _cluster_num: next cluster number to assign
    @type _cluster_num: L{int}
    @ivar _operationbuffer: buffer for not-yet-completable operations
    @type _operationbuffer: L{pyzim.operationbuffer.OperationBuffer} or L{None}
    @ivar _processors: list of processors to that have been installed on this zim
    @type _processors: L{list} of L{pyzim.processor.BaseProcessor}
    @ivar _counter: the counter counting mimetype occurences
    @type _counter: L{pyzim.counter.Counter}
    """
    def __init__(self, f, offset=0, mode="r", policy=DEFAULT_POLICY):
        """
        The default constructor for opening a ZIM file.

        Multiple modes are supported:
            - "r": read-only
            - "w": create a new file for writing, truncating the old file
            - "u"/"a": modify the existing file

        @param f: file-like object to read from (NOTE: must support reading)
        @type f: file-like object
        @param offset: offset of the ZIM archive within the file.
        @type offset: L{int}
        @param mode: in which mode to open the ZIM file (e.g. read)
        @type mode: L{str}
        @param policy: policy to use, default to L{pyzim.policy.DEFAULT_POLICY}
        @type policy: L{pyzim.policy.Policy}
        @raises ValueError: on invalid value for a parameter
        @raises TypeError: on invalid type for value
        """
        if not isinstance(offset, int):
            raise TypeError("Expected an int, got {} instead!".format(type(offset)))
        if offset < 0:
            raise ValueError("Offset must be at least 0, got {} instead!".format(offset))
        if not isinstance(mode, str):
            raise TypeError("Mode must be a string, got {} instead!".format(type(mode)))
        if not isinstance(policy, Policy):
            raise TypeError("Policy must be a pyzim.policy.Policy instance, got {} instead!".format(type(policy)))
        ModifiableMixIn.__init__(self)
        self._f = f
        self._base_offset = offset
        self._mode = mode
        self.policy = policy
        self._closed = False
        self.filelock = threading.Lock()
        self._processors = []

        self._init_caches()

        if self._mode in ("r", "u", "a"):
            # this is not a new file
            new_file = False
            # need to load header, mimetypes, pointerlist, ...
            if self._mode == "r":
                # read only
                logger.info("Opening ZIM archive in read-only mode....")
                self._writable = self.mutable = False
            else:
                # write allowed
                logger.info("Opening ZIM archive in update/modify mode...")
                self._writable = self.mutable = True
            self._load_header()
            self._load_mimetypelist()
            self._load_pointerlists()
            self.spaceallocator = SpaceAllocator(file_end=self.header.checksum_position)
        elif self._mode == "w":
            # this is a new file
            new_file = True
            # create new header, mimetypelist, pointerlists, ...
            logger.info("Creating a new ZIM archive...")
            self._writable = True
            with self.filelock:
                self._f.seek(self._base_offset)
                self._f.truncate()
        else:
            raise ValueError("Unrecognized mode: '{}'!".format(mode))

        if self._writable:
            # instantiate compression strategy, ...
            logger.debug("Instantiating writing-related components...")
            self.compression_strategy = self.policy.compression_strategy_class(
                zim=self,
                **self.policy.compression_strategy_kwargs,
            )
            self.uncompressed_compression_strategy = self.policy.uncompressed_compression_strategy_class(
                zim=self,
                **self.policy.uncompressed_compression_strategy_kwargs,
            )
            self._operation_buffer = OperationBuffer(self)
        else:
            # no need to instantiate a compression strategy or related components
            self.compression_strategy = None
            self.uncompressed_compression_strategy = None
            self._operation_buffer = None
        if new_file:
            # finalize init for new archive, which requires compression strategy to be initialized
            self._init_new()
        self._counter = Counter.load_from_archive(self, how=self.policy.counter)
        if self._counter is not None:
            self.install_processor(self._counter)
        self.after_flush_or_read()
        logger.info("ZIM archive ready.")

    @classmethod
    def open(cls, path, mode="r", offset=0, policy=DEFAULT_POLICY):
        """
        Open the Zim archive at the specified path.

        In addition to the modes listed in the documentation of L{pyzim.archive.Zim.__init__},
        the mode "x" is also supported. It behaves like mode "w", but
        raises an exception should the file already exists.

        @param path: path to open
        @type path: L{str}
        @param mode: mode of the Zim archive (currently, only reading is supported)
        @type mode: L{str}
        @param offset: offset of the ZIM archive within the file.
        @type offset: L{int}
        @param policy: policy to use, default to L{pyzim.policy.DEFAULT_POLICY}
        @type policy: L{pyzim.policy.Policy}
        @return: the Zim archive opened from the file
        @rtype: L{pyzim.archive.Zim}
        @raises FileExistsError: if mode == "x" and path already exists
        @raises ValueError: on invalid mode
        """
        assert isinstance(path, str)
        assert isinstance(mode, str)
        assert isinstance(policy, Policy)
        if mode == "r":
            fmode = "rb"
        elif mode == "w":
            fmode = "w+b"
        elif mode == "x":
            if os.path.exists(path):
                raise FileExistsError("Path '{}' already exists!".format(path))
            fmode = "w+b"
            mode = "w"
        elif mode in ("u", "a"):
            fmode = "r+b"
        else:
            raise ValueError("Unrecognized mode: '{}'!".format(mode))
        f = open(path, fmode)
        archive = cls(f, mode=mode, policy=policy)
        return archive

    def _init_caches(self):
        """
        Initializes internal caches according to policy.
        """
        logger.debug("Initializing caches...")
        logger.debug(
            "Cluster cache: {} with kwargs {}.".format(
                self.policy.cluster_cache_class,
                self.policy.cluster_cache_kwargs,
            ),
        )
        self.cluster_cache = self.policy.cluster_cache_class(
            on_leave=self._on_cluster_cache_leave,
            **self.policy.cluster_cache_kwargs,
        )
        logger.debug(
            "Entry cache: {} with kwargs {}.".format(
                self.policy.entry_cache_class,
                self.policy.entry_cache_kwargs,
            ),
        )
        self.entry_cache = self.policy.entry_cache_class(
            on_leave=self._on_entry_cache_leave,
            **self.policy.entry_cache_kwargs,
        )
        logger.debug("Caches initialized.")

    def _load_header(self):
        """
        Read the header.
        """
        logger.debug("Reading archive header...")
        with self.filelock:
            # read header/meta data from file
            self.header = Header.from_file(self._f, seek=self._base_offset)
        self.header.mutable = self._writable
        self.add_submodifiable(self.header)
        logger.debug(
            "Archive header read ({} clusters, {} entries).".format(
                self.header.cluster_count,
                self.header.entry_count,
            ),
        )

    def _load_mimetypelist(self):
        """
        Load the mimetypelist.
        """
        logger.debug("Reading mimetype list...")
        assert self.header is not None
        pos = self._base_offset + self.header.mime_list_position
        with self.filelock:
            self.mimetypelist = MimeTypeList.from_file(self._f, seek=pos)
        self.mimetypelist.mutable = self._writable
        self.add_submodifiable(self.mimetypelist)
        logger.debug("Mimetype list read ({} mimetypes).".format(len(self.mimetypelist)))

    def _load_pointerlists(self):
        """
        Load the URL and title pointer lists.
        """
        assert self.header is not None
        logger.debug("Reading pointerlists...")
        url_pointer_position = self._base_offset + self.header.url_pointer_position
        cluster_pointer_position = self._base_offset + self.header.cluster_pointer_position
        # the URL pointer list
        self._url_pointer_list = self.policy.ordered_pointer_list_class.from_zim_file(
            self,
            self.header.entry_count,
            key_func=self._get_full_url_for_entry_at,
            seek=url_pointer_position,
        )
        logger.debug("URL pointer list has {} entries.".format(len(self._url_pointer_list)))
        # the cluster pointer list
        self._cluster_pointer_list = self.policy.simple_pointer_list_class.from_zim_file(
            self,
            self.header.cluster_count,
            seek=cluster_pointer_position,
        )
        logger.debug("Cluster pointer list has {} entries.".format(len(self._cluster_pointer_list)))
        self._url_pointer_list.mutable = self._writable
        self.add_submodifiable(self._url_pointer_list)
        self._cluster_pointer_list.mutable = self._writable
        self.add_submodifiable(self._cluster_pointer_list)
        # also set current cluster number
        self._cluster_num = len(self._cluster_pointer_list)
        # releasing lock, as we are accessing these via higher level APIs
        # the entry title pointer list
        # this may either be part of the header or accessed
        # via an URL
        if self.has_entry_for_full_url(constants.URL_ENTRY_TITLE_INDEX):
            logger.debug("Reading entry title pointer list via URL...")
            self._entry_title_pointer_list = self.policy.title_pointer_list_class.from_zim_entry(
                self,
                constants.URL_ENTRY_TITLE_INDEX,
                key_func=self._get_namespace_title_for_entry_by_url_index,
            )
        else:
            # unfortunately, we need to fall back to the header title pointer position
            # this requires us to re-acquire the file lock
            logger.debug("Entry title pointer list not available via URL, falling back to header information...")
            self._cluster_pointer_list = self.policy.title_pointer_list_class.from_zim_file(
                self,
                self.header.entry_count,
                seek=self.header.title_pointer_position,
                key_func=self._get_namespace_title_for_entry_by_url_index,
            )
        logger.debug("Entry title pointer list has {} entries.".format(len(self._entry_title_pointer_list)))
        self._entry_title_pointer_list.mutable = self._writable
        self.add_submodifiable(self._entry_title_pointer_list)
        # the article title pointer list
        if self.has_entry_for_full_url(constants.URL_ARTICLE_TITLE_INDEX):
            self._article_title_pointer_list = self.policy.title_pointer_list_class.from_zim_entry(
                self,
                constants.URL_ARTICLE_TITLE_INDEX,
                key_func=self._get_title_for_entry_by_url_index,
            )
            logger.debug("Article title pointer list has {} entries.".format(len(self._article_title_pointer_list)))
        else:
            logger.debug("No article title pointer list found, creating a new one")
            self._article_title_pointer_list = self.policy.title_pointer_list_class.new(key_func=self._get_title_for_entry_by_url_index)
            if self._writable:
                # add a placeholder item for the list, so we can later
                # set the blob directly
                article_title_pointer_placeholder_item = Item(
                    namespace="X",
                    url=constants.URL_ARTICLE_TITLE_INDEX[1:],  # remove namespace
                    mimetype=constants.MIMETYPE_ZIMLISTING,
                    blob_source=EmptyBlobSource(),
                    is_article=False,
                )
                self.add_item(article_title_pointer_placeholder_item, force_uncompressed=True)
                self.uncompressed_compression_strategy.flush()
        self._article_title_pointer_list.mutable = self._writable
        self.add_submodifiable(self._article_title_pointer_list)
        logger.debug("Pointerlists loaded.")

    def _init_new(self):
        """
        Initiate as a new, empty archive.

        This instantiated the header, pointerlists, ... .

        TODO: find a better name for this method.
        """
        logger.debug("Creating initial components for an empty archive...")
        self.header = Header.placeholder()
        self.mimetypelist = MimeTypeList([])
        self._url_pointer_list = self.policy.ordered_pointer_list_class.new(key_func=self._get_full_url_for_entry_at)
        self._cluster_pointer_list = self.policy.simple_pointer_list_class.new()
        self._entry_title_pointer_list = self.policy.title_pointer_list_class.new(key_func=self._get_namespace_title_for_entry_by_url_index)
        self._article_title_pointer_list = self.policy.title_pointer_list_class.new(key_func=self._get_title_for_entry_by_url_index)
        # add them all as mutable submodifiables and mark them as dirty
        submodifiables = [
            self.header,
            self.mimetypelist,
            self._url_pointer_list,
            self._cluster_pointer_list,
            self._entry_title_pointer_list,
            self._article_title_pointer_list,
        ]
        for submodifiable in submodifiables:
            self.add_submodifiable(submodifiable)
            submodifiable.mutable = True
            submodifiable.mark_dirty()
        # calculate the amount of space needed at the start of the archive
        # ensure there is enough space for the header at the start
        reserved_space_at_start = Header.LENGTH
        # currently, the ZIM standard unfortunately requires us to reserve
        # some space at the start
        if self.policy.reserve_mimetype_space is not None:
            reserved_space_at_start += self.policy.reserve_mimetype_space
        self.spaceallocator = SpaceAllocator(file_end=reserved_space_at_start)
        # write dummy header, so that we have space reserved for the proper header
        with self.filelock:
            self._f.seek(0)
            self._f.write(self.header.to_bytes())
        self._cluster_num = 0
        logger.debug("Writing placeholder entry for v0 title index...")
        # we insert a placeholder entry for the title index, so that
        # the various lists already contain a pointer and we have
        # an associated blob and cluster.
        title_pointer_placeholder_item = Item(
            namespace="X",
            url=constants.URL_ENTRY_TITLE_INDEX[1:],  # remove namespace
            mimetype=constants.MIMETYPE_ZIMLISTING,
            blob_source=EmptyBlobSource(),
            is_article=False,
        )
        # we also do this for the article pointer list
        article_title_pointer_placeholder_item = Item(
            namespace="X",
            url=constants.URL_ARTICLE_TITLE_INDEX[1:],  # remove namespace
            mimetype=constants.MIMETYPE_ZIMLISTING,
            blob_source=EmptyBlobSource(),
            is_article=False,
        )
        self.add_item(title_pointer_placeholder_item, force_uncompressed=True)
        self.add_item(article_title_pointer_placeholder_item, force_uncompressed=True)
        # ensure placeholders are written
        self.uncompressed_compression_strategy.flush()
        logger.debug("Initial components ready.")

    def _get_full_url_for_entry_at(self, location):
        """
        Return the full URL for the entry with at the specified location.

        This is used as the key function for the URL pointer list.

        @param location: location of the entry in the ZIM file
        @type location: L{int}
        @return: the full URL of the specified entry
        @rtype: L{bytes}
        """
        should_allow_cache_replacement = not self._writable
        entry = self.get_entry_at(location, allow_cache_replacement=should_allow_cache_replacement)
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
        # TODO:
        # due to some dead-lock problems, we can't allow cache replacement here
        # basically, getting the entry from the cache may result in this
        # entry being cached and another one being kicked out of the cache
        # if the ZIM is being written, the entry dirty and autoflush is
        # enabled, this would cause the entry to maybe be added to the
        # title list, which would in turn try to acquire the lock, but
        # this method is called from within this lock.
        # to prevent this, we disable cache replacements. But it would
        # be better, if we still allow the caching, so that the entries
        # most relevant to binary search are stored.
        should_allow_cache_replacement = not self._writable
        entry = self.get_entry_by_url_index(i, allow_cache_replacement=should_allow_cache_replacement)
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
        should_allow_cache_replacement = not self._writable
        entry = self.get_entry_by_url_index(i, allow_cache_replacement=should_allow_cache_replacement)
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
        logger.info("Closing ZIM archive...")
        for processor in self._processors:
            processor.before_close()
        if self.dirty:
            # we need to flush changes
            self.flush()
        logger.debug("Closing underlying file object...")
        self._f.close()
        self._closed = True
        for processor in self._processors:
            processor.after_close()
        logger.info("Archive closed.")

    @property
    def closed(self):
        """
        Return True if this archive has already been closed, False otherwise.

        @return: whether this archive has already been closed or not
        @rtype: L{bool}
        """
        return self._closed

    def flush(self):
        """
        Write all changes to disk.

        @raises pyzim.exceptions.ZimFileClosed: when the ZIM file is already closed.
        @raises pyzim.exceptions.NonMutable: if this ZIM file is set to be non-mutable
        """
        self._check_closed()
        self.ensure_mutable()

        logger.info("Flushing archive...")

        # call processors
        for processor in self._processors:
            processor.before_flush()

        # first, we need to clear all caches, so that cached clusters
        # and entries get written and any other changed (e.g. url) cause
        # the related components to be marked as dirty
        logger.debug("Clearing caches...")
        self.cluster_cache.clear()
        self.entry_cache.clear()

        # if the archive is not dirty, no further modifications are needed
        if not self.dirty:
            logger.info("No changes to archive, skipping flush.")
            return

        # Log helper -  print dirty state
        logger.debug("Dirty state at the start of flush():")
        for submodifiable in self._submodifiables:
            if submodifiable.dirty:
                logger.debug("Submodifiable {} is dirty".format(repr(submodifiable)))

        # flush our compression strategy so that all our entries and
        # clusters get written
        # NOTE: we do this once more later on
        logger.debug("Flushing compression strategies...")
        self.compression_strategy.flush()
        self.uncompressed_compression_strategy.flush()
        # apply any remaining operations that required all entries to be added
        # TODO: this would be preferable after the title pointer lists have been written.
        logger.debug("Applying buffered operations depending on entries...")
        self._operation_buffer.finalize_entry_dependent_operations()

        logger.debug("Flushing processors...")
        for processor in self._processors:
            processor.after_content_flush()

        # we need to do the following for each component of this ZIM excluding the header:
        # - check if it is dirty
        # - free the space
        # - allocate new space
        # - write it
        # - change positions

        # components we need to flush (excl. header):
        # - article title pointer list (before url ptr list and mimetypelist)
        # - mimetypelist
        # - cluster pointer list
        # - entry title pointer list
        # - url pointer list

        # article title pointer list
        if self._article_title_pointer_list.dirty:
            logger.debug("Writing article title pointer list...")
            article_title_pointer_entry = self.get_entry_by_full_url(constants.URL_ARTICLE_TITLE_INDEX)
            article_title_pointer_entry.set_content(InMemoryBlobSource(self._article_title_pointer_list.to_bytes())).flush()
            self._article_title_pointer_list.after_flush_or_read()
            # TODO: delete old list
        # entry title pointer list
        # after article pointer list, as that title is included here too
        if self._entry_title_pointer_list.dirty:
            logger.debug("Writing entry title pointer list...")
            self.compression_strategy.flush()
            self.uncompressed_compression_strategy.flush()
            # the URL and title will already be part of the lists, as these
            # entries are added as placeholders when initiating as new
            entry_title_pointer_entry = self.get_entry_by_full_url(constants.URL_ENTRY_TITLE_INDEX)
            entry_title_cluster = entry_title_pointer_entry.set_content(InMemoryBlobSource(self._entry_title_pointer_list.to_bytes()))
            entry_title_cluster.flush()
            # calculate and set offset
            # this should be:
            # - the offset of the cluster
            # - 1 for the infobyte
            # - the offset of the first blob
            entry_title_cluster_offset = self._cluster_pointer_list.get_by_index(entry_title_pointer_entry.cluster_number)
            entry_title_blob_offset = entry_title_cluster.get_offset(entry_title_pointer_entry.blob_number)
            self.header.title_pointer_position = entry_title_cluster_offset + 1 + entry_title_blob_offset
            self._entry_title_pointer_list.after_flush_or_read()

        # mimetypelist
        if self.mimetypelist.dirty:
            logger.debug("Writing mimetypelist for {} mimetypes...".format(len(self.mimetypelist)))
            new_mtl_size = self.mimetypelist.get_disk_size()
            if self.policy.reserve_mimetype_space is None:
                # we can write the mimetype list anywhere
                # thus, we also need to free any previous space
                old_mtl_size = self.mimetypelist.get_unmodified_disk_size()
                self.spaceallocator.mark_free(self.header.mime_list_position, old_mtl_size)
                new_mtl_position = self.spaceallocator.allocate(new_mtl_size)
            else:
                # we need to write the mimetype list directly after the header
                if new_mtl_size > self.policy.reserve_mimetype_space:
                    # not enough space reserved
                    raise ZimWriteException(
                        "Mimetypelist requires {} bytes, only {} reserved by policy!".format(
                            new_mtl_size,
                            self.policy.reserve_mimetype_space,
                        )
                    )
                new_mtl_position = 80
            logger.debug("Writing mimetypelist of {} bytes to offset {}".format(new_mtl_size, new_mtl_position))
            with self.acquire_file() as f:
                f.seek(self._base_offset + new_mtl_position)
                f.write(self.mimetypelist.to_bytes())
            self.header.mime_list_position = new_mtl_position
            self.mimetypelist.after_flush_or_read()

        # cluster pointer list
        if self._cluster_pointer_list.dirty:
            logger.debug("Writing cluster pointer list for {} clusters...".format(len(self._cluster_pointer_list)))
            new_cpl_size = self._cluster_pointer_list.get_disk_size()
            old_cpl_size = self._cluster_pointer_list.get_unmodified_disk_size()
            self.spaceallocator.mark_free(self.header.cluster_pointer_position, old_cpl_size)
            new_cpl_position = self.spaceallocator.allocate(new_cpl_size)
            with self.acquire_file() as f:
                f.seek(self._base_offset + new_cpl_position)
                f.write(self._cluster_pointer_list.to_bytes())
            self.header.cluster_pointer_position = new_cpl_position
            self.header.cluster_count = len(self._cluster_pointer_list)
            self._cluster_pointer_list.after_flush_or_read()

        # url pointer list
        if self._url_pointer_list.dirty:
            logger.debug("Writing URL pointer list for {} entries...".format(len(self._url_pointer_list)))
            new_upl_size = self._url_pointer_list.get_disk_size()
            old_upl_size = self._url_pointer_list.get_unmodified_disk_size()
            self.spaceallocator.mark_free(self.header.url_pointer_position, old_upl_size)
            new_upl_position = self.spaceallocator.allocate(new_upl_size)
            with self.acquire_file() as f:
                f.seek(self._base_offset + new_upl_position)
                f.write(self._url_pointer_list.to_bytes())
            self.header.url_pointer_position = new_upl_position
            self.header.entry_count = len(self._url_pointer_list)
            self._url_pointer_list.after_flush_or_read()

        # at this point we need to update the checksum position
        self.header.checksum_position = self.spaceallocator.file_end

        # lastly, we do the same for the header, but write it at offset 0
        if self.header.dirty:
            logger.debug("Writing header...")
            # as location and size are always fixed, we can skip space
            # allocation
            with self.acquire_file() as f:
                f.seek(self._base_offset)
                f.write(self.header.to_bytes())
            self.header.after_flush_or_read()

        # nearly done, now we just need to update the checksum
        self.update_checksum()
        # and truncate any excess bytes
        if self.policy.truncate:
            logger.debug("Truncating file...")
            with self.acquire_file() as f:
                f.seek(self.header.checksum_position + 16)
                f.truncate()
        self.after_flush_or_read()
        # call processors
        for processor in self._processors:
            processor.after_flush()
        logger.debug("Archive flushed.")

    def __enter__(self):
        """
        Called upon entering a with-statement. Provides self as object for the context.
        """
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        """
        Called upon exiting a with-statement. Closes self.
        """
        if exc_type is not None:
            logger.error("An exception occured inside the ZIM context!", exc_info=True)
        if not self.closed:
            self.close()
        return False

    @contextlib.contextmanager
    def acquire_file(self):
        """
        A context manager that locks the file access and provides the wrapped file object for the context.

        @raises pyzim.exceptions.ZimFileClosed: when the ZIM file is already closed.
        """
        self._check_closed()
        with self.filelock:
            yield self._f

    def _on_cluster_cache_leave(self, cluster_offset, cluster):
        """
        Called when a cluster leaves the cache.

        If the archive is writable and autoflush is enabled, write the
        cluster if it is dirty.

        @param cluster_offset: total offset of cluster
        @type cluster_offset: L{int}
        @param cluster: the cluster leaving the cache
        @type cluster: L{pyzim.cluster.Cluster}
        """
        assert isinstance(cluster_offset, int) and cluster_offset >= 0
        if self._writable and self.policy.autoflush:
            # cluster needs to be written if it is dirty
            # split this in two if statements as .dirty may not be
            # available if archive is not writable
            if cluster.dirty:
                # write cluster
                cluster_num = self.get_cluster_index_by_offset(cluster_offset - self._base_offset)
                logger.log(constants.LOG_LEVEL_WRITE, "Autoflushing cluster {} at {}".format(cluster_num, cluster_offset))
                self.write_cluster(cluster, cluster_num=cluster_num)

    def _on_entry_cache_leave(self, full_location, entry):
        """
        Called when an entry leaves the cache.

        If the archive is writable and autoflush is enabled, write the
        entry if it is dirty.

        @param full_location: the full offset of the entry
        @type full_location: L{int}
        @param entry: the entry leaving the cache
        @type entry: L{pyzim.entry.BaseEntry}
        """
        if self._writable and self.policy.autoflush and entry.dirty:
            # entry should be written to zim file
            logger.log(constants.LOG_LEVEL_WRITE, "Autoflushing entry for {} at {}".format(entry.url, full_location))
            self.write_entry(entry)

    def _new_cluster_num(self):
        """
        Return the number of the next new cluster.

        This also increments the internal counter.

        @return: the number of the next cluster
        @rtype: L{int}
        """
        # TODO: thread safety
        cluster_num = self._cluster_num
        self._cluster_num += 1
        return cluster_num

    @property
    def counter(self):
        """
        Return the counter used for counting mimetype occurences.

        If not counter is available, return L{None} instead.

        @return: the mimetype counter
        @rtype: L{pyzim.counter.Counter} or L{None}
        """
        # we do not directly expose self._counter because setting the counter
        # also requires the handler to be installed. The read-only property
        # prevents the user from setting the counter directly
        return self._counter

    # ============= ModifiableMixIn methods =========

    def get_disk_size(self):
        # the checksum position in the header points to 16 bytes before
        # the end of the archive
        # we can neglect the base offset here, as it is not relevant.
        return self.header.checksum_position + 16

    # ============= entry functions ==========

    def get_entry_at(self, location, bind=True, allow_cache_replacement=True):
        """
        Return the entry at the specified location (offset) in the ZIM file.

        If caching is configured, an instance of a previous entry may be
        returned. This entry may already be modified and/or bound (even
        if C{bind=False}).

        @param location: location/offset of the entry in the ZIM file
        @type location: L{int}
        @param bind: if nonzero (default), bind this entry
        @type bind: L{bool}
        @param allow_cache_replacement: if nonzero (default), allow cached entries to be replaced
        @type allow_cache_replacement: L{bool}
        @return: the entry at the specified location
        @rtype: L{pyzim.entry.BaseEntry}
        """
        assert isinstance(location, int) and location >= 0
        # call processors
        for processor in self._processors:
            processor.before_entry_get(location=location, allow_cache_replacement=allow_cache_replacement)
        # get entry
        full_location = self._base_offset + location
        logger.log(constants.LOG_LEVEL_READ, "Getting entry at {} (full offset {})".format(location, full_location))
        if self.entry_cache.has(full_location):
            logger.log(constants.LOG_LEVEL_READ, "Entry found in cache.")
            entry = self.entry_cache.get(full_location)
        else:
            with self.filelock:
                entry = BaseEntry.from_file(self._f, seek=full_location)
            self.entry_cache.push(full_location, entry, allow_replacement=allow_cache_replacement)
        if bind:
            entry.bind(self)
        # call processors
        for processor in self._processors:
            entry = processor.after_entry_get(
                location=location,
                allow_cache_replacement=allow_cache_replacement,
                entry=entry,
            )
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

    def get_entry_by_url_index(self, i, allow_cache_replacement=True):
        """
        Return the entry at the specified index in the URL pointer list.

        @param i: index of entry in URL pointer list
        @type i: L{int}
        @return: the entry at the specified location
        @param allow_cache_replacement: if nonzero (default), allow cached entries to be replaced
        @type allow_cache_replacement: L{bool}
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
        return self.get_entry_at(location, allow_cache_replacement=allow_cache_replacement)

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

    def _update_url_pointers(self, start, diff, edit_etpl=True, edit_atpl=True, update_redirects=True, skip=()):
        """
        Update references to URL pointers.

        As several pointers point to the position of an entry within the
        URL pointer list, but said list is sorted, modifying it will
        likely cause said pointers to point to the wrong entries. This
        method takes care of updating said references.

        @param start: lowest URL pointer index that needs updating
        @type start: L{int}
        @param diff: integer to update said references by (e.g. C{1})
        @type diff: L{int}
        @param edit_etpl: if nonzero (default), update the entry title pointer list
        @type edit_etpl: L{bool}
        @param edit_atpl: if nonzero (default), update the article title pointer list
        @type edit_atpl: L{bool}
        @param update_redirects: if nonzero (default), update redirects
        @type update_redirects: L{bool}
        @param skip: list or tuple of full urls not to update recursively
        @type skip: L{list} or L{tuple} of L{str}
        """
        assert isinstance(start, int) and start >= 0
        assert isinstance(diff, int)
        assert isinstance(edit_etpl, bool)
        assert isinstance(edit_atpl, bool)
        assert isinstance(skip, (list, tuple))
        logger.log(constants.LOG_LEVEL_WRITE, "Updating pointers (start={}, diff={})".format(start, diff))
        # update entry title list
        if edit_etpl:
            self._entry_title_pointer_list.mass_update(diff, start=start)
        # update article title list
        if edit_atpl:
            self._article_title_pointer_list.mass_update(diff, start=start)
        # update redirects
        if update_redirects:
            for entry in self.iter_entries_by_url():
                if entry.is_redirect and (entry.full_url not in skip):
                    if entry.redirect_index >= start:
                        entry.redirect_index += diff
                        assert entry.redirect_index != (self._url_pointer_list.get(entry.full_url))
                        self.write_entry(entry)
        # update header
        if self.header.main_page is not None:
            if self.header.main_page > start:
                self.header.main_page += diff

    def set_mainpage_url(self, url):
        """
        Set the mainpage url.

        An entry for the specified url must already exists.

        @param url: non-full url of the mainpage (the mainpage is always in the C{"C"} namespace). Set to L{None} to disable.
        @type url: L{str} or L{None}
        @raises TypeError: on type error
        @raises pyzim.exceptions.ZimFileClosed: if archive is already closed
        @raises pyzim.exceptions.NonMutable: if this zim file is not mutable
        """
        if (not isinstance(url, str)) and (url is not None):
            raise TypeError("Expected a string or None, got {} instead!".format(type(url)))
        self._check_closed()
        self.ensure_mutable()

        logger.debug("Setting mainpage URL to: {}".format(url))

        # remove previous entry
        try:
            self.remove_entry_by_full_url(constants.URL_MAINPAGE_REDIRECT)
            logger.debug("Removed previous mainpage redirect entry.")
        except EntryNotFound:
            # no previous mainpage redirect
            pass
        if url is None:
            logger.debug("Removing mainpage reference.")
            self.header.main_page = None
            # it is possible, albeit unlikely, that such an operation
            # is currently buffered. We counteract this case by also
            # buffering this set operation
            self._operation_buffer.buffer_set_mainpage_url(None)
        else:
            full_url = "C" + url
            self.add_full_url_redirect(constants.URL_MAINPAGE_REDIRECT, full_url)
            try:
                self.header.main_page = self._url_pointer_list.get_index(constants.URL_MAINPAGE_REDIRECT)
            except KeyError:
                # it is possible that the previous add_full_url_redirect
                # operation had to be buffered. In this case, the index
                # we need to set in the header is not yet available.
                # the solution is that we buffer this operation again.
                # this will cause this method to be called again later.
                logger.debug("Target for mainpage redirect not found, buffering operation.")
                self._operation_buffer.buffer_set_mainpage_url(url)

    def remove_entry_by_full_url(self, full_url, blob="empty"):
        """
        Remove the entry at the specified url.

        You can specify how the associated blob should be treated using
        the C{blob} parameter:

            - C{"keep"}: do nothing
            - C{"empty"}: empty the associated blob (see L{pyzim.cluster.ModifiableClusterWrapper.empty_blob})
            - C{"remove"}: delete the blob. Be warned that this will likely cause issues with other indexes.

        If the entry has an associated blob, the cluster will be flushed.

        Redirects pointing towards this url will also be removed. Buffered
        operations may interfere with this behavior, so be sure to flush()
        before.

        @param full_url: full url of entry to remove
        @type full_url: L{str}
        @param blob: how to treat the associated blob
        @type blob: L{str}
        @raises TypeError: on type error
        @raises ValueErorr: on value error.
        @raises pyzim.exceptions.EntryNotFound: if the target entry does not exist
        @raises pyzim.exceptions.ZimFileClosed: if archive is already closed
        @raises pyzim.exceptions.NonMutable: if this zim file is not mutable
        """
        if not isinstance(full_url, str):
            raise TypeError("Expected a string, got '{}' instead!".format(type(full_url)))
        if not isinstance(blob, str):
            raise TypeError("Expected a string, got '{}' instead!".format(type(blob)))
        if blob not in ("keep", "empty", "remove"):
            raise ValueError("Invalid value for parameter 'blob': expected 'keep', 'empty' or *remove', not '{}'!".format(blob))
        self._check_closed()
        self.ensure_mutable()
        # in order to delete an entry, we need to:
        # - find the entry
        # - process the associated blob
        # - find and process redirects pointing to this entry
        # - if entry is an article, remove it from the article title list
        # - remove it from the regular title list
        # - remove it from the url list

        logger.log(constants.LOG_LEVEL_WRITE, "Removing entry at '{}'...".format(full_url))

        # call processors
        for processor in self._processors:
            processor.before_entry_remove(full_url=full_url, blob=blob)

        # find entry
        # find offset and index in url pointer list
        # it is slightly more efficient to call these methods individually
        # and recycle information
        try:
            url_index = self._url_pointer_list.get_index(full_url)
        except KeyError:
            raise EntryNotFound("No entry for URL: {}".format(full_url))
        # we have the index already, so using get_by_index() should be faster
        offset = self._url_pointer_list.get_by_index(url_index)
        entry = self.get_entry_at(offset)
        entry_size = entry.get_unmodified_disk_size()  # unmodified, as we want the size on disk
        # process blob
        if blob != "keep" and not entry.is_redirect:
            cluster = self.get_cluster_by_index(entry.cluster_number)
            if blob == "empty":
                cluster.empty_blob(entry.blob_number)
            elif blob == "remove":
                cluster.remove_blob(entry.blob_number)
            else:  # pragma: no cover
                raise RuntimeError("Unreachable state reached!")
            cluster.flush()
        # before we modify the various pointer lists, remove redirects pointing to this entry.
        # we store the redirects to remove in a list because removing
        # them directly would interfer with the pointers used by .iter_entries()
        redirects_to_remove = []
        for redirect in self.iter_entries():
            if redirect.is_redirect and redirect.redirect_index == url_index:
                if self.get_entry_by_url_index(redirect.redirect_index).full_url == redirect.full_url:
                    # due to the pointer modifications, it can happen that a redirect
                    # temporarily points to itself
                    # in this case, we should not remove it
                    continue
                redirects_to_remove.append(redirect.full_url)
        # remove article title
        # explicitly check for namespace, because entry_at_url_is_article
        # may otherwise raise an exception
        is_article = (entry.namespace == "C" and self.entry_at_url_is_article(full_url))
        if is_article:
            article_index = self._article_title_pointer_list.get_by_pointer(url_index)
            self._article_title_pointer_list.remove_by_index(article_index)
        # remove entry title
        entry_title_index = self._entry_title_pointer_list.get_by_pointer(url_index)
        self._entry_title_pointer_list.remove_by_index(entry_title_index)
        # remove from url list
        logger.debug("Removing index {} from url pointer list (pointer: {})".format(url_index, self._url_pointer_list._pointers[url_index]))  # DEBUG
        self._url_pointer_list.remove_by_index(url_index)
        logger.debug("after removal: {}".format(self._url_pointer_list._pointers))
        # update entry cache
        self.entry_cache.remove(self._base_offset + offset)
        # update header
        if self.header.main_page == url_index:
            self.header.main_page = None
        # update other pointers
        self._update_url_pointers(start=url_index, diff=-1, edit_atpl=is_article)
        # mark space as free
        self.spaceallocator.mark_free(offset, entry_size)
        # remove redirects pointing to this url
        for r_url in redirects_to_remove:
            logger.log(constants.LOG_LEVEL_WRITE, "Removing redirect for URL {} as it points to removed entry {}".format(r_url, entry.full_url))
            self.remove_entry_by_full_url(r_url)

        # call processors
        for processor in self._processors:
            processor.after_entry_remove(
                full_url=full_url,
                blob=blob,
                entry=entry,
                is_article=is_article,
            )

    def entry_at_url_is_article(self, full_url):
        """
        Check if the entry at the specified full url is an article.

        Articles are always in C namespace, thus the full url must start
        with a C.

        This method returns False if the entry does not exists at all.

        @param full_url: full url of entry to check
        @type full_url: L{str}
        @return: whether the entry is an article or not
        @rtype: L{bool}
        @raises TypeError: on type error
        @raises ValueErorr: on value error.
        @raises pyzim.exceptions.ZimFileClosed: if archive is already closed
        """
        if not isinstance(full_url, str):
            raise TypeError("full_url must be a string, got {} instead!".format(type(full_url)))
        if not full_url.startswith("C"):
            raise ValueError("Per definition all articles must be in the 'C' namespace, but URL '{}' points to another namespace!".format(full_url))
        self._check_closed()
        try:
            url_index = self._url_pointer_list.get_index(full_url)
        except KeyError:
            # entry is not yet registered at all, returning False
            return False
        try:
            self._article_title_pointer_list.get_by_pointer(url_index)
        except KeyError:
            # entry is not in article list
            return False
        else:
            # entry is in article list
            return True

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

    def iter_entries_by_url(self, start=None, end=None):
        """
        Iterate over all entries in this ZIM, ordered by full URL.

        If start and end are specified, they reference the indexes of the
        first (inclusive) and last (exclusive) entry to return.
        In other words, this behavior matches the l[start:end] syntax.

        @param start: index of first entry to return (inclusive)
        @type start: L{int}
        @param end: index of last entry to return (exclusive)
        @type end: L{int}
        @yield: the entries in the specified range
        @ytype: L{pyzim.entry.BaseEntry}
        """
        for pos in self._url_pointer_list.iter_pointers(start=start, end=end):
            yield self.get_entry_at(pos)

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

    def write_entry(self, entry, update_redirects=True, add_to_title_pointer_list=True):
        """
        Write an entry to this archive.

        @param entry: entry to write
        @type entry: L{pyzim.entry.BaseEntry}
        @param update_redirects: if nonzero, update redirects to this article if necessary
        @type update_redirects: L{bool}
        @param add_to_title_pointer_list: if nonzero (default), add the entry to the title pointer lists
        @type add_to_title_pointer_list: L{bool}
        @raises TypeError: on type error
        @raises pyzim.exceptions.ZimFileClosed: if archive is already closed
        @raises pyzim.exceptions.NonMutable: if this zim file is not mutable
        @raises pyzim.exceptions.BindingError: if entry is not bound to self
        """
        if not isinstance(entry, BaseEntry):
            raise TypeError("Expected an instance of BaseEntry (or subclasss), got {} instead!".format(type(entry)))
        update_redirects = bool(update_redirects)
        add_to_title_pointer_list = bool(add_to_title_pointer_list)

        self._check_closed()
        self.ensure_mutable()
        if entry.zim is not self:
            raise BindingError("Entry '{}' is not bound to this archive!".format(entry))

        logger.log(constants.LOG_LEVEL_WRITE, "Writing entry for url: {}".format(entry.full_url))

        # call processors
        for processor in self._processors:
            entry = processor.before_entry_write(
                entry=entry,
                add_to_title_pointer_list=add_to_title_pointer_list,
                update_redirects=update_redirects,
            )

        # Approach for writing an entry:
        # We have two main cases: the entry is completely new or we are updating an existing entry
        # the update itself must consider the following potential changes:
        #  - change of entry size
        #  - change of title
        #  - change of article status
        #  - change of URL
        #  - general changes to attributes
        #
        # We perform the write in three main stages:
        #  1. gather information about the entry and changes that need
        #     to be performed. That is because performing the changes may
        #     make it significantly harder to extract information about
        #     what other changes may need to be performed (e.g. indexes changed)
        #  2. write the new entry and mark the previous one as free
        #  3. perform other required changes (update redirects, title lists, url pointer lists, ...)

        # ================= Stage 1: gather information ================
        old_url = entry.unmodified_full_url
        is_article = entry.is_article
        new_size = entry.get_disk_size()
        # check if this is a new entry or an existing one
        try:
            old_url_index = self._url_pointer_list.get_index(old_url)
        except KeyError:
            # no previous entry at the old URL
            # this entry is new
            old_url_index = None
            old_offset = None
            disk_size_changed = True
            is_new_entry = True
            old_entry = None
        else:
            # entry was already part of the archive
            old_offset = self._url_pointer_list.get_by_index(old_url_index)
            old_entry = self.get_entry_at(old_offset, allow_cache_replacement=False)
            # PROBLEM: if we override an entry with a new entry with the
            # same URL but a different title, the calculated old disk
            # size. This is a temp fix.
            old_size = old_entry.get_unmodified_disk_size()
            # old_size = entry.get_unmodified_disk_size()
            disk_size_changed = (old_size != new_size)
            is_new_entry = False
        url_changed = (old_url != entry.full_url) or is_new_entry

        # find title pointers
        if not is_new_entry:
            try:
                old_entry_title_index = self._entry_title_pointer_list.get_by_pointer(old_url_index)
            except KeyError:
                # now, this is an annoying edge case
                # later on, when we insert this entries URL into the
                # URL pointer list, we cause all other references (title pointers,
                # redirects, ...) to go out of sync. We fix this by updating
                # said references, including the redirects. However, at
                # that point the redirect is not yet added to the title
                # pointer list (because the mass update method we are
                # using for convenience would cause the pointer to be off)
                # thus, this method may be recursively called when the
                # entry is already part of the archive but not yet added
                # to the entry title pointer list (but only if it is a
                # redirect!)
                if not entry.is_redirect:  # pragma: no cover
                    # unexpected situation, re-raise error
                    raise
                old_entry_title_index = None
            try:
                old_article_title_index = self._article_title_pointer_list.get_by_pointer(old_url_index)
            except KeyError:
                # entry was not in article title list
                old_article_title_index = None

        # find redirects that reference this entry
        redirects_to_update = []  # list of redirects pointing to the old url index
        if update_redirects and url_changed and (not is_new_entry):
            for redirect in self.iter_entries():
                if redirect.is_redirect and redirect.redirect_index == old_url_index:
                    redirects_to_update.append(redirect.full_url)

        # ================= Stage 2: write entry =======================
        # mark previous space as free
        if disk_size_changed and (not is_new_entry):
            self.spaceallocator.mark_free(old_offset, old_size)
        if disk_size_changed:
            # get a new location to write entry to
            new_offset = self.spaceallocator.allocate(new_size)
        else:
            # reuse old location
            new_offset = old_offset
        # write entry
        with self.acquire_file() as f:
            f.seek(self._base_offset + new_offset)
            f.write(entry.to_bytes())
        # remove dirty status from entry
        entry.after_flush_or_read()
        # update the cache
        self.entry_cache.push(self._base_offset + new_offset, entry)

        # ================= Stage 3: perform changes ===================
        # delete from title lists
        if not is_new_entry:
            if old_entry_title_index is not None:
                # see definition in stage 1
                self._entry_title_pointer_list.remove_by_index(old_entry_title_index)
            if old_article_title_index is not None:
                self._article_title_pointer_list.remove_by_index(old_article_title_index)
        # delete old URL pointer
        if (url_changed or disk_size_changed) and (not is_new_entry):
            self._url_pointer_list.remove_by_index(old_url_index)
            self._update_url_pointers(start=old_url_index, diff=-1)
        # insert new URL pointer, which may cause this entry itself to be updated
        if url_changed or disk_size_changed:
            new_url_index = self._url_pointer_list.add(entry.full_url, new_offset)
            self._update_url_pointers(start=new_url_index, diff=1, update_redirects=True, skip=(entry.full_url, ))
            need_entry_update = (entry.is_redirect and entry.redirect_index >= new_url_index)
            if need_entry_update:
                # the redirect pointer of the redirect is outdated, write again
                entry.redirect_index += 1
                with self.acquire_file() as f:
                    f.seek(self._base_offset + new_offset)
                    f.write(entry.to_bytes())
                entry.after_flush_or_read()
                self.entry_cache.push(self._base_offset + new_offset, entry)
            assert (not entry.is_redirect) or (entry.redirect_index != new_url_index)
        else:
            # using previous URL
            new_url_index = old_url_index
        # insert new titles
        if add_to_title_pointer_list:
            self._entry_title_pointer_list.add(entry.namespace + entry.title, new_url_index)
            if is_article:
                self._article_title_pointer_list.add(entry.title, new_url_index)

        # update redirects that point to previous index location
        if old_url_index != new_url_index:
            for redirect_full_url in redirects_to_update:
                logger.log(constants.LOG_LEVEL_WRITE, "Updating redirect '{}' to point to {}...".format(redirect_full_url, new_url_index))
                redirect = self.get_entry_by_full_url(redirect_full_url)
                redirect.redirect_index = new_url_index
                self.write_entry(redirect, update_redirects=False)

        # call processors
        for processor in self._processors:
            processor.after_entry_write(
                entry=entry,
                old_entry=old_entry,
                old_offset=old_offset,
                new_offset=new_offset,
                is_new_entry=is_new_entry,
                add_to_title_pointer_list=add_to_title_pointer_list,
                update_redirects=update_redirects,
            )
        if entry.is_redirect:
            logger.debug("{} ({} / {})".format(entry.full_url, entry.redirect_index, self.get_entry_by_url_index(entry.redirect_index).full_url))  # DEBUG

    def add_redirect(self, source, target, title=None):
        """
        Add a redirect from the source (non-full) url to the target (non-full) url.

        This method uses non-full urls and operates in the C{"C"} namespace.
        Use L{pyzim.archive.Zim.add_full_url_redirect} to work with full urls.

        @param source: non-full url to redirect from
        @type source: L{str}
        @param target: non-full url to redirect to
        @type target: L{str}
        @param title: title for the redirect, defaulting to the target entry title
        @type title: L{str} or L{None}
        @raises TypeError: on type error
        @raises ValueError: on invalid value
        @raises pyzim.exceptions.EntryNotFound: if target url does not yet exists
        @raises pyzim.exceptions.ZimFileClosed: if archive is already closed
        @raises pyzim.exceptions.NonMutable: if this zim file is not mutable
        """
        if not isinstance(source, str):
            raise TypeError("Expected a string, got {} instead!".format(type(source)))
        if not isinstance(target, str):
            raise TypeError("Expected a string, got {} instead!".format(type(target)))
        if (not isinstance(title, str)) and (title is not None):
            raise TypeError("Expected a string or None, got {} instead!".format(type(title)))
        if source == target:
            raise ValueError("Infinite self-redirect at url {}".format(source))
        self._check_closed()
        self.ensure_mutable()

        full_source = "C" + source
        full_target = "C" + target
        self.add_full_url_redirect(full_source, full_target, title=title)

    def add_full_url_redirect(self, source, target, title=None):
        """
        Add a redirect from the source (full) url to the target (full) url.

        This method uses full urls. You'll likely want to use L{pyzim.archive.Zim.add_redirect}
        if you want to work with non-full urls in the C{"C"} namespace.

        Be warned that a redirect that can not be resolved will be buffered.
        This will not only result in an increased memory usage, but may
        also cause an exception to be raised later on if the url redirect
        can not be resolved during the next flush.

        @param source: full url to redirect from
        @type source: L{str}
        @param target: full url to redirect to
        @type target: L{str}
        @param title: title for the redirect, defaulting to the target entry title
        @type title: L{str} or L{None}
        @raises TypeError: on type error
        @raises ValueError: on invalid value
        @raises pyzim.exceptions.ZimFileClosed: if archive is already closed
        @raises pyzim.exceptions.NonMutable: if this zim file is not mutable
        """
        if not isinstance(source, str):
            raise TypeError("Expected a string, got {} instead!".format(type(source)))
        if not isinstance(target, str):
            raise TypeError("Expected a string, got {} instead!".format(type(target)))
        if (not isinstance(title, str)) and (title is not None):
            raise TypeError("Expected a string or None, got {} instead!".format(type(title)))
        if source == target:
            raise ValueError("Infinite self-redirect at url {}".format(source))
        if (not source) or (not target):
            raise ValueError("Can not work with empty URLs!")
        self._check_closed()
        self.ensure_mutable()

        try:
            target_index = self._url_pointer_list.get_index(target)
        except KeyError:
            # target entry not found, but it may currently be buffered
            # in the compression strategy and not yet written
            # thus, we buffer the redirect, which will cause this method
            # to be called on the next flush
            self._operation_buffer.buffer_add_redirect(source, target, title)
            return
        if title is None:
            title = self.get_entry_by_full_url(target).title
        entry = RedirectEntry(
            namespace=source[0],
            url=source[1:],
            revision=0,
            redirect_index=target_index,
            title=title,
            parameters=[],
        )
        entry.bind(self)
        # call processors
        for processor in self._processors:
            processor.on_add_redirect(entry=entry)
        # write entry
        self.write_entry(entry)

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

        If caching is configured, an instance of a previous cluster may
        be returned. This entry may already be modified and/or bound
        (even if C{bind=False}).

        @param location: location/offset of the cluster in the ZIM file
        @type location: L{int}
        @return: the entry at the specified location
        @rtype: L{pyzim.cluster.Cluster}
        """
        # not offering bind as a parameter as that would make this function useless
        # call processors
        for processor in self._processors:
            processor.before_cluster_get(location=location)

        full_location = self._base_offset + location
        logger.log(constants.LOG_LEVEL_READ, "Loading cluster at {} (full offset {})...".format(location, full_location))
        if self.cluster_cache.has(full_location):
            logger.log(constants.LOG_LEVEL_READ, "Cluster found in cache,")
            cluster = self.cluster_cache.get(full_location)
        else:
            cluster_class = self.policy.cluster_class
            cluster = cluster_class(zim=self, offset=full_location)
            if self._writable:
                # wrap cluster to make it writable
                logger.log(constants.LOG_LEVEL_READ, "Wrapping cluster to allow modifications...")
                cluster = ModifiableClusterWrapper(cluster)
            self.cluster_cache.push(full_location, cluster)
        # call processors
        for processor in self._processors:
            cluster = processor.after_cluster_get(cluster=cluster)
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

    def remove_cluster_by_index(self, i):
        """
        Remove the cluster with the specified index.

        @param i: index of cluster to remove
        @type i: L{int}
        """
        assert isinstance(i, int) and i >= 0
        pos = self._cluster_pointer_list.get_by_index(i)
        full_location = self._base_offset + pos
        cluster = self.get_cluster_by_index(i)
        try:
            self.cluster_cache.remove(full_location, call_on_leave=False)
        except KeyError:
            # cluster was not cached
            pass
        size = cluster.get_unmodified_disk_size()  # last size on disk
        self.spaceallocator.mark_free(pos, size)
        if i == len(self._cluster_pointer_list) - 1:
            # we can remove that pointer completely
            self._cluster_pointer_list.remove_by_index(i)
        else:
            # deleting this value would mess up all other indexes
            # so instead, we change the pointer to mark to the same
            # location as another cluster
            # we use the last cluster, as that one is safely not this
            # cluster due to the preceding if/else check
            placeholder_index = len(self._cluster_pointer_list) - 1
            placeholder_pointer = self._cluster_pointer_list.get_by_index(placeholder_index)
            self._cluster_pointer_list.set(i, placeholder_pointer, add_placeholders=False)

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
        This is not the full offset (base offset must be substracted manually).

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

    def write_cluster(self, cluster, cluster_num=None):
        """
        Update an existing cluster in this zim.

        The cluster must already be part of this archive. Use L{Zim.new_cluster}
        for creating new clusters.

        @param cluster: cluster to write
        @type cluster: L{ModifiableClusterWrapper}
        @param cluster_num: the number/id of the cluster. Providing it speeds up the method.
        @return: the cluster number
        @rtype: L{int}
        @raises TypeError: on type error
        @raises ValueError: on invalid values (e.g. negative cluster numbers)
        @raises pyzim.exceptions.ZimFileClosed: if archive is already closed
        @raises pyzim.exceptions.NonMutable: if this zim file is not mutable
        @raises pyzim.exceptions.BindingError: if cluster is not bound to self
        """
        if not isinstance(cluster, ModifiableClusterWrapper):
            raise TypeError("Expected a ModifiableClusterWrapper, got {} instead!".format(type(cluster)))
        if not (isinstance(cluster_num, int) or cluster_num is None):
            raise TypeError("Expected an integer or None as cluster number, got {} instead!".format(type(cluster_num)))
        if cluster_num is not None and (cluster_num < 0):
            raise ValueError("Cluster number may not be negative!")
        self.ensure_mutable()
        self._check_closed()
        if cluster.zim is not self:
            raise BindingError("Cluster {} is not bound to this archive!".format(repr(cluster)))

        # call processors
        for processor in self._processors:
            cluster = processor.before_cluster_write(cluster=cluster)

        # first, figure out if size has changed
        is_new_cluster = (cluster.offset is None)
        old_size = cluster.get_unmodified_disk_size()
        new_size = cluster.get_disk_size()
        if is_new_cluster:
            logger.log(constants.LOG_LEVEL_WRITE, "Writing new cluster...")
            cluster_num = self._new_cluster_num()
        elif cluster_num is None:
            cluster_num = self.get_cluster_index_by_offset(cluster.offset)
            logger.log(constants.LOG_LEVEL_WRITE, "Writing existing cluster, previously located at {}".format(cluster.offset))
        logger.log(constants.LOG_LEVEL_WRITE, "Cluster number is {}".format(cluster_num))
        # secondly, find a new position for the cluster
        # it would be nice if we could write the cluster to the same
        # location as before, but we can only free and modify the area
        # after the cluster has been written, as we may still need to
        # access and decompress old data
        old_offset = cluster.offset
        new_offset = self.spaceallocator.allocate(new_size)
        logger.log(constants.LOG_LEVEL_WRITE, "Writing {} bytes of cluster at {}".format(new_size, new_offset))
        # third, write the cluster to it's (perhaps new) location
        # now, this may seem a bit counterintuitive, but we only acquire
        # the file whenever we write a chunk of data (thus also requiring
        # a reseek). This is because reading said data chunk may itself
        # require the acquisition of the file lock, which would cause
        # a dead lock
        pos = new_offset
        for chunk in cluster.iter_write():
            with self.acquire_file() as f:
                f.seek(self._base_offset + pos)
                f.write(chunk)
                pos += len(chunk)
        # at this point also update the cache
        if new_offset != old_offset:
            self.cluster_cache.remove(old_offset, call_on_leave=False)
        self.cluster_cache.push(new_offset, cluster)
        # fourth, update cluster position and cluster pointer list
        cluster.offset = new_offset
        self._cluster_pointer_list.set(cluster_num, cluster.offset, add_placeholders=True)
        # fifth, inform the cluster of the change
        cluster.after_flush_or_read()
        # finally, mark previously used space as free
        if old_offset is not None:
            self.spaceallocator.mark_free(old_offset, old_size)
        # call processors
        for processor in self._processors:
            processor.after_cluster_write(
                cluster=cluster,
                old_offset=old_offset,
                new_offset=new_offset,
                cluster_number=cluster_num,
            )
        # return the cluster number
        return cluster_num

    def new_cluster(self):
        """
        Add a new cluster to this archive.

        NOTE: the cluster will not be cached until it is written at least once.
        Consequently, the autoflush function will not work until you've
        written them at least once.

        @return: a new cluster
        @rtype: L{pyzim.cluster.ModifiableClusterWrapper}
        @raises pyzim.exceptions.ZimFileClosed: if archive is already closed
        @raises pyzim.exceptions.NonMutable: if this zim file is not mutable
        """
        self.ensure_mutable()
        self._check_closed()

        cluster = EmptyCluster(self)
        wrapped_cluster = ModifiableClusterWrapper(cluster)
        wrapped_cluster.bind(self)
        return wrapped_cluster

    # =============== metadata interface =============

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
        @raises pyzim.exceptions.ZimFileClosed: if archive is already closed
        """
        if self._writable:
            # this ZIM file is writable, which means that there are
            # perhaps some buffered writing operations for the metadata
            # we need to flush them first
            # TODO: this is rather inefficient. We should only do this
            # if a buffered entry makes changes in the "M" namespace
            self.flush()
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

    def set_metadata(self, key, value, mimetype="text/plain"):
        """
        Set metadata of the ZIM archive.

        @param key: key of metadata to set
        @type key: L{str}
        @param value: value of metadata to set
        @type value: L{str} or L{bytes}
        @param mimetype: mimetype of the associated blob
        @type mimetype: L{str} or L{bytes}
        @raises TypeError: on type error
        @raises ValueError: on invalid value
        @raises pyzim.exceptions.ZimFileClosed: if archive is already closed
        @raises pyzim.exceptions.NonMutable: if this zim file is not mutable
        """
        if not isinstance(key, str):
            raise TypeError("Expected a string, got '{}' instead!".format(type(key)))
        if not isinstance(value, (str, bytes)):
            raise TypeError("Expected a string or bytes, got '{}' instead!".format(type(value)))
        if not isinstance(mimetype, str):
            raise TypeError("Expected a string, got '{}' instead!".format(type(mimetype)))
        if not key:
            raise ValueError("Mimetype key can not be empty!")
        self._check_closed()
        self.ensure_mutable()

        item = Item(
            namespace="M",
            url=key,
            mimetype=mimetype,
            blob_source=InMemoryBlobSource(value),
        )
        self.add_item(item)

    # TODO: illustration support -> PIL/Pillow

    # =========== item interface ===============

    def add_item(self, item, force_uncompressed=False):
        """
        Add an item to this archive.

        The write may not happen immediately.

        @param item: item to write
        @type item: L{pyzim.item.Item}
        @param force_uncompressed: if nonzero, add the item to the compression strategy for uncompressed content, regardless of other options
        @type force_uncompressed: L{bool}
        @raises TypeError: on type error
        @raises pyzim.exceptions.ZimFileClosed: if archive is already closed
        @raises pyzim.exceptions.NonMutable: if this zim file is not mutable
        """
        if not isinstance(item, Item):
            raise TypeError("Expected an Item, got {} instead!".format(type(item)))
        self._check_closed()
        self.ensure_mutable()

        if not force_uncompressed:
            self.compression_strategy.add_item(item)
        else:
            self.uncompressed_compression_strategy.add_item(item)
        self.mark_dirty()

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

    def update_checksum(self):
        """
        Calculate and write the checksum.

        NOTE: this prior to this, L{pyzim.header.Header.checksum_position}
        should already be set to the new position and the header flushed.
        This method does not take care of this.
        """
        logger.debug("Updating checksum...")
        new_checksum = self.calculate_checksum()
        with self.acquire_file() as f:
            f.seek(self.header.checksum_position)
            f.write(new_checksum)

    # ============ other interfaces ===============

    def install_processor(self, processor):
        """
        Install a processor on this archive.

        See L{pyzim.processor} for more details.

        @param processor: processor to install
        @type processor: L{bool}
        @raise TypeError: on type error
        """
        if not isinstance(processor, BaseProcessor):
            raise TypeError("Expected a pyzim.processor.BaseProcessor, got {} instead!".format(type(processor)))
        processor.on_install(self)
        self._processors.append(processor)


# Fix for pydoctor, which would otherwise hide all names exported in __init__.__all__
__all__ = ["Zim"]
