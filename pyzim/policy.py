"""
Policies for class behavior.

Policies are basically configurations managing the behavior of various
classes and which classes are choosen. For example, a policy may specify
whether a cluster should be decompressed all at once or only as needed.

Generally speaking, policies should not have any effect on the content
of a ZIM file, but may affect the structure. An example would be the
choosen compression level and type for a cluster.

@var DEFAULT_POLICY: the default policy to use
@type DEFAULT_POLICY: L{Policy}
@var LOW_RAM_DECOMP_POLICY: A policy for minimizing RAM usage  during decompression
@type LOW_RAM_DECOMP_POLICY: L{Policy}
@var HIGH_PERFORMANCE_DECOMP_POLICY: a policy for maximizing performance during decompression
@type HIGH_PERFORMANCE_DECOMP_POLICY: L{Policy}

@var ALL_POLICIES: a list of all policies defined in this module, used for testing
@type ALL_POLICIES: L{list} of L{Policy}
"""
from . import constants
from .cluster import Cluster, OffsetRememberingCluster, InMemoryCluster
from .compression import CompressionTarget
from .compressionstrategy import BaseCompressionStrategy, SimpleCompressionStrategy
from .cache import BaseCache, NoOpCache, LastAccessCache, HybridCache


class Policy(object):
    """
    A policy is a configuration that influences the behavior of various
    pyzim classes, mostly in regards to resource management.

    @ivar compression_options: options for to pass to L{pyzim.compression.BaseCompressionInterface}
    @type compression_options: L{dict}
    @ivar cluster_class: cluster implemenetation to use
    @type cluster_class: a class (L{pyzim.cluster.Cluster} or a subclass)
    @ivar entry_cache_class: class to use for caching entries
    @type entry_cache_class: a subclass of L{pyzim.cache.BaseCache}
    @ivar entry_cache_kwargs: keyword arguments to pass to the entry_cache_class
    @type entry_cache_kwargs: L{dict}
    @ivar cluster_cache_class: class to use for caching clusters
    @type cluster_cache_class: a subclass of L{pyzim.cache.BaseCache}
    @ivar cluster_cache_kwargs: keyword arguments to pass to the cluster_cache_class
    @type cluster_cache_kwargs: L{dict}
    @ivar compression_strategy_class: compression strategy to use when writing new items
    @type compression_strategy_class: L{pyzim.compressionstrategy.BaseCompressionStrategy}
    @ivar compression_strategy_kwargs: kwargs of compression strategy to use (excluding C{"zim"})
    @type compression_strategy_kwargs: L{dict}
    @ivar autoflush: automatically write modified clusters and entries. Requires caches to be used. NOTE: cache size should at leat be 2 in this case!
    @type autoflush: L{bool}
    @ivar truncate: if nonzero, truncate when flushing the file
    @type truncate: L{bool}
    @ivar reserve_mimetype_space: number of bytes after the header to reserve for the mimetypelist, L{None} to disable. Will likely be removed in the future.
    @type reserve_mimetype_space: L{int} or L{None}
    """
    def __init__(
        self,
        compression_options={},
        cluster_class=None,
        entry_cache_class=None,
        entry_cache_kwargs=None,
        cluster_cache_class=None,
        cluster_cache_kwargs=None,
        compression_strategy_class=None,
        compression_strategy_kwargs=None,
        autoflush=True,
        truncate=False,
        reserve_mimetype_space=2048,
    ):
        """
        The default constructor.

        @param compression_options: options for to pass to L{pyzim.compression.BaseCompressionInterface}
        @type compression_options: L{dict}
        @param cluster_class: cluster implemenetation to use
        @type cluster_class: a class (L{pyzim.cluster.Cluster} or a subclass) or L{None}
        @param entry_cache_class: class to use for caching entries
        @type entry_cache_class: a subclass of L{pyzim.cache.BaseCache} or L{None}
        @param entry_cache_kwargs: keyword arguments to pass to the entry_cache_class
        @type entry_cache_kwargs: L{dict} or L{None}
        @param cluster_cache_class: class to use for caching clusters
        @type cluster_cache_class: a subclass of L{pyzim.cache.BaseCache} or L{None}
        @param cluster_cache_kwargs: keyword arguments to pass to the cluster_cache_class
        @type cluster_cache_kwargs: L{dict} or L{None}
        @param compression_strategy_class: compression strategy to use when writing new items
        @type compression_strategy_class: subclass of L{pyzim.compressionstrategy.BaseCompressionStrategy} or L{None}
        @ivar compression_strategy_kwargs: kwargs of compression strategy to use (excluding C{"zim"})
        @type compression_strategy_kwargs: L{dict}
        @param autoflush: automatically write modified clusters and entries. Requires caches to be used. NOTE: cache size should at leat be 2 in this case!
        @type autoflush: L{bool}
        @param truncate: if nonzero, truncate when flushing the file
        @type truncate: L{bool}
        @param reserve_mimetype_space: number of bytes after the header to reserve for the mimetypelist, L{None} to disable. Will likely be removed in the future.
        @type reserve_mimetype_space: L{int} or L{None}
        """
        assert isinstance(compression_options, dict)
        if cluster_class is None:
            cluster_class = OffsetRememberingCluster
        assert issubclass(cluster_class, Cluster)
        if entry_cache_class is None:
            entry_cache_class = NoOpCache
            if entry_cache_kwargs is None:
                entry_cache_kwargs = {}
        elif entry_cache_kwargs is None:
            entry_cache_kwargs = {}
        assert issubclass(entry_cache_class, BaseCache)
        assert isinstance(entry_cache_kwargs, dict)
        if cluster_cache_class is None:
            cluster_cache_class = LastAccessCache
            if cluster_cache_kwargs is None:
                cluster_cache_kwargs = {"max_size": 2}
        elif cluster_cache_kwargs is None:
            cluster_cache_kwargs = {"max_size": 2}
        assert issubclass(cluster_cache_class, BaseCache)
        assert isinstance(cluster_cache_kwargs, dict)
        if compression_strategy_class is None:
            compression_strategy_class = SimpleCompressionStrategy
            # if no kwargs are set, use specific defaults
            if compression_strategy_kwargs is None:
                compression_strategy_kwargs = {
                    "compression_type": constants.DEFAULT_COMPRESSION,
                }
        assert issubclass(compression_strategy_class, BaseCompressionStrategy)
        if compression_strategy_kwargs is None:
            compression_strategy_kwargs = {}
        assert isinstance(compression_strategy_kwargs, dict)
        assert "zim" not in compression_strategy_kwargs
        assert (reserve_mimetype_space is None) or (isinstance(reserve_mimetype_space, int) and reserve_mimetype_space > 2)

        self.compression_options = compression_options
        self.cluster_class = cluster_class
        self.entry_cache_class = entry_cache_class
        self.entry_cache_kwargs = entry_cache_kwargs
        self.cluster_cache_class = cluster_cache_class
        self.cluster_cache_kwargs = cluster_cache_kwargs
        self.compression_strategy_class = compression_strategy_class
        self.compression_strategy_kwargs = compression_strategy_kwargs
        self.autoflush = autoflush
        self.truncate = truncate
        self.reserve_mimetype_space = reserve_mimetype_space


DEFAULT_POLICY = Policy()
LOW_RAM_DECOMP_POLICY = Policy(
    compression_options={
        "general.target": CompressionTarget.LOWRAM_DECOMPRESSION,
    },
    cluster_class=Cluster,
    cluster_cache_class=NoOpCache,
    entry_cache_class=NoOpCache,
)
HIGH_PERFORMANCE_DECOMP_POLICY = Policy(
    compression_options={
        "general.target": CompressionTarget.FASTEST_DECOMPRESSION,
    },
    cluster_class=InMemoryCluster,
    cluster_cache_class=HybridCache,
    cluster_cache_kwargs={"last_cache_size": 8, "top_cache_size": 8},
    entry_cache_class=HybridCache,
    entry_cache_kwargs={"last_cache_size": 8, "top_cache_size": 128},
)

ALL_POLICIES = [
    DEFAULT_POLICY,
    LOW_RAM_DECOMP_POLICY,
    HIGH_PERFORMANCE_DECOMP_POLICY,
]
