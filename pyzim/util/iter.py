"""
Iteration utilities.
"""
from ..archive import Zim


def iter_by_cluster(zim):
    """
    Iterate over all entries in an archive, yielding their full URLs
    grouped by cluster and sorted by blob number.

    This method serves as a way to more efficiently decompress all data
    within an archive. When coupled with a bit of caching and the right
    cluster type (e.g. L{pyzim.cluster.OffsetRememberingCluster}, iterating
    using this method prevents a cluster from being uncompressed more
    than once.

    NOTE: this method reads all entries in a ZIM before it starts
    iterating. Consequently, this method may have a significant I/O
    overhead and RAM usage.

    Redirects are yielded as their own group after the other groups.

    @param zim: ZIM archive to iterate iver
    @type zim: L{pyzim.archive.Zim}
    @yields: tuple of URLs of entries, one tuple per cluster, each URL sorted by blob number
    @ytype: L{tuple} of L{str}
    @raises TypeError: on type error
    """
    if not isinstance(zim, Zim):
        raise TypeError("Expected a Zim, got {} instead!".format(type(zim)))
    cluster_num2urls = {}  # map cluster_num -> [(blob_num, url), ...]
    # group entries
    for entry in zim.iter_entries_by_url():
        url = entry.full_url
        if entry.is_redirect:
            cluster_num = -1
            blob_num = -1
        else:
            cluster_num = entry.cluster_number
            blob_num = entry.blob_number
        if cluster_num not in cluster_num2urls:
            cluster_num2urls[cluster_num] = []
        cluster_num2urls[cluster_num].append((blob_num, url))
    # yield regular entries
    cluster_nums = sorted(cluster_num2urls.keys())
    for cluster_num in cluster_nums:
        if cluster_num == -1:
            # we yield the redirects later
            continue
        entries = cluster_num2urls[cluster_num]
        entries = tuple(e[1] for e in sorted(entries))
        yield entries
    # yield the redirects
    yield tuple(e[1] for e in cluster_num2urls[-1])
