"""
The compression strategy handles the bundling of blobs into clusters.

@var logger: logger used by the compression strategy
@type logger: L{logging.Logger}
"""
import threading
import logging

from pyzim import constants
from pyzim.compression import CompressionType
from pyzim.item import Item


logger = logging.getLogger(__name__)


class BaseCompressionStrategy(object):
    """
    Baseclass for compression strategies.

    A compression strategy is responsible for deciding how blobs should#
    be bundled into clusters. For this, they take L{pyzim.item.Item}
    instances (which also grants them additional informations like
    mimetype), decide which cluster they should belong to, instantiate
    entries and set the proper cluster and blob numbers.

    L{BaseCompressionStrategy} instances are supposed to be used in a
    recursive hierarchy. For example, one compression strategy could take
    an item, analyzes it for its mimetype, then entries of each mimetype
    grouped together to another compression strategy (instantiated once
    for each mimetype) that puts all items below a specific size into
    an uncompressed cluster.

    @ivar zim: zim archive this compression strategy compresses for
    @type zim: L{pyzim.archive.Zim}
    """
    def __init__(self, zim):
        """
        The default constructor.

        @param zim: zim archive this compression strategy compresses for
        @type zim: L{pyzim.archive.Zim}
        """
        self.zim = zim

    def has_items(self):
        """
        True if this compression strategy still has items to be written.

        @return: whether there are some items that still need to be written
        @rtype: L{bool}
        """
        raise NotImplementedError("Subclasses of BaseCompressionStrategy need to implement has_items()!")

    def add_item(self, item):
        """
        Handle the addition of an item (e.g. create entries, ...).

        @param item: item to add
        @type item: L{pyzim.item.Item}
        """
        assert isinstance(item, Item)
        raise NotImplementedError("Subclasses of BaseCompressionStrategy need to implement handle_item()!")

    def flush(self):
        """
        Flush all current items.

        This should create and flush all remaining clusters and entries.
        """
        raise NotImplementedError("Subclasses of BaseCompressionStrategy need to implement flush()!")


class SimpleCompressionStrategy(BaseCompressionStrategy):
    """
    A compression strategy that compresses into a single cluster.

    New clusters are created if the current cluster's uncompressed size
    would exceed L{SimpleCompressionStrategy.max_size}.

    @ivar compression_type: compression type to use
    @type compression_type: L{pyzim.compression.CompressionType}
    @ivar entries: list of entries to compress
    @type entries: L{list} of L{pyzim.entry.BaseEntry}
    @ivar cluster: current cluster
    @type cluster: L{pyzim.cluster.ModifiableClusterWrapper}
    @ivar max_size: max (uncompressed) size the cluster should have. This may be exceeded.
    @type max_size: L{int}

    @ivar _lock: thread safety lock
    @type _lock: L{threading.Lock}
    """
    def __init__(self, zim, compression_type, max_size=2*1024*1024):
        """
        The default constructor.

        @param zim: zim archive this compression strategy compresses for
        @type zim: L{pyzim.archive.Zim}
        @param compression_type: compression type to use
        @type compression_type: L{pyzim.compression.CompressionType}
        @param max_size: max (uncompressed) size the cluster should have. This may be exceeded.
        @type max_size: L{int}
        """
        assert isinstance(compression_type, CompressionType)
        BaseCompressionStrategy.__init__(self, zim)

        self.compression_type = compression_type
        self.max_size = max_size
        self.entries = []
        self._lock = threading.Lock()
        self._new_cluster()

    def has_items(self):
        return len(self.entries) > 0

    def _new_cluster(self):
        """
        Instantiate a new cluster.

        The new cluster is in L{SimpleCompressionStrategy.cluster}. This
        method does not take care of flushing the previous cluster.
        """
        logger.log(constants.LOG_LEVEL_COMPRESSION_STRATEGY, "Creating a new cluster")
        self.cluster = self.zim.new_cluster()
        self.cluster.compression = self.compression_type

    def _finalize_current_cluster(self):
        """
        Finalize the current cluster, writing it and the various items.
        """
        logger.log(constants.LOG_LEVEL_COMPRESSION_STRATEGY, "Finalizing current cluster...")
        cluster_num = self.zim.write_cluster(self.cluster)
        self._new_cluster()
        for entry in self.entries:
            entry.cluster_number = cluster_num
            self.zim.write_entry(entry)
        self.entries = []

    def _new_cluster_needed(self, item):
        """
        Check if a new cluster is needed for the item.

        @param item: item to check
        @type item: L{pyzim.item.Item}
        @return: True if the current cluster should be finalized.
        @rtype: L{bool}
        """
        assert isinstance(item, Item)
        # cluster should be finalized if it already contains at least
        # on blob and the new item would put it above max size
        if not self.entries:
            # no entry currently, use current cluster
            return False
        current_size = self.cluster.get_total_decompressed_size()
        item_size = item.blob_source.get_size()
        new_size = current_size + item_size
        if new_size > self.max_size:
            # size would be exceeded, use new cluster
            return True
        return False

    def add_item(self, item):
        assert isinstance(item, Item)
        with self._lock:
            if self._new_cluster_needed(item):
                # fianlize the current cluster
                self._finalize_current_cluster()
            entry = item.to_entry(self.zim)
            blob_source = item.blob_source
            blob_num = self.cluster.append_blob(blob_source)
            entry.blob_number = blob_num
            self.entries.append(entry)

    def flush(self):
        # check if there are any items we should flush
        if self.entries:
            with self._lock:
                self._finalize_current_cluster()


class MimetypeBasedCompressionStrategy(BaseCompressionStrategy):
    """
    A compression strategy utilizing mimetypes.

    @ivar mimetype2cs: a dict mapping mimetype -> compression strategy
    @type mimetype2cs: L{dict} of L{str} -> L{BaseCompressionStrategy}
    @ivar cs_class: compression strategy to use for each mimetype
    @type cs_class: a subclass of L{BaseCompressionStrategy}
    @ivar cs_kwargs: kwargs to pass to each new instance of cs_class (ignoring C{"zim"})
    @type cs_kwargs: L{dict}

    @ivar _lock: thread safety lock
    @type _lock: L{threading.Lock}
    """
    def __init__(self, zim, cs_class=SimpleCompressionStrategy, cs_kwargs={}):
        """
        The default constructor.

        @param zim: zim archive this compression strategy compresses for
        @type zim: L{pyzim.archive.Zim}
        @param cs_class: compression strategy to use for each mimetype
        @type cs_class: a subclass of L{BaseCompressionStrategy}
        @param cs_kwargs: kwargs to pass to each new instance of cs_class
        @type cs_kwargs: L{dict}
        """
        assert issubclass(cs_class, BaseCompressionStrategy)
        assert isinstance(cs_kwargs, dict) and ("zim" not in cs_kwargs)

        BaseCompressionStrategy.__init__(self, zim)
        self.cs_class = cs_class
        self.cs_kwargs = cs_kwargs
        self.mimetype2cs = {}
        self._lock = threading.Lock()

    def has_items(self):
        for cs in self.mimetype2cs.values():
            if cs.has_items():
                return True
        return False

    def _get_cs(self, mimetype):
        """
        Get the compression strategy for the specified mimetype.

        @param mimetype: mimetype to get compression strategy for
        @type mimetype: L{str}
        @return: the compression strategy to use
        @rtype: L{BaseCompressionStrategy}
        """
        assert isinstance(mimetype, str)
        with self._lock:
            if mimetype in self.mimetype2cs:
                # cs already instantiaded
                return self.mimetype2cs[mimetype]
            # new cs needed
            new_cs = self.cs_class(zim=self.zim, **self.cs_kwargs)
            self.mimetype2cs[mimetype] = new_cs
            return new_cs

    def add_item(self, item):
        assert isinstance(item, Item)
        # find which strategy to use
        mimetype = item.mimetype
        cs = self._get_cs(mimetype)
        # add item
        cs.add_item(item)

    def flush(self):
        # propagate flush() to substrategies
        with self._lock:
            for cs in self.mimetype2cs.values():
                cs.flush()
