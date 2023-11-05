"""
Processors provide a way to monitor and manipulate various events and actions happening in a ZIM archive.

For example, a processor can profile the time certain events take, keep
track of the number of entries with certain attributes in the archive,
change an item title and more.
"""


class BaseProcessor(object):
    """
    Base class for processors.

    Each method will be called during certain operations of the Zim archive.
    They should all take any number of keyword arguments (C{**kwargs}) as
    we expect more arguments to be changed over time. Most of the
    default implementations of these methods are NO-OP.

    Some methods allow you to return a modified value. Beware that more
    than one L{BaseProcessor} may return modified values, thus you can
    not be sure that the value you receive is actually the unmodified
    original value. Ideally, you should write your processor in such a
    way that subsequent processors can work on it too.

    @ivar zim: zim archive this processor is bound to
    @type zim: L{pyzim.archive.Zim}
    """
    def on_install(self, zim, **kwargs):
        """
        Called when this processor is installed to a ZIM file.

        By default, this sets L{BaseProcessor.zim}.

        @param zim: ZIM archive this processor is installed on
        @type zim: L{pyzim.archive.Zim}
        @param kwargs: extra keyword arguments
        @type kwargs: L{dict}
        """
        self.zim = zim

    def before_close(self, **kwargs):
        """
        Called when the archive will be closed.

        @param kwargs: extra keyword arguments
        @type kwargs: L{dict}
        """
        pass

    def after_close(self, **kwargs):
        """
        Called when the archive has been closed.

        @param kwargs: extra keyword arguments
        @type kwargs: L{dict}
        """
        pass

    def on_add_redirect(self, **kwargs):
        """
        Called when a redirect will be added.

        Keyword arguments:

            - C{entry} (L{pyzim.entry.RedirectEntry}): redirect entry that will be added

        @param kwargs: extra keyword arguments
        @type kwargs: L{dict}
        """
        pass

    def before_cluster_get(self, **kwargs):
        """
        Called when L{pyzim.archive.Zim.get_cluster_at} was called.

        This is called at the beginning of said method.

        Keyword arguments:

            - C{location} (L{int}): location/offset of the cluster to load

        @param kwargs: extra keyword arguments
        @type kwargs: L{dict}
        """
        pass

    def after_cluster_get(self, **kwargs):
        """
        Called when L{pyzim.archive.Zim.get_cluster_at} was called.

        The cluster may have been retrieved from the cache or read from
        disk.

        Keyword arguments:

            - C{cluster} (L{pyzim.cluster.Cluster}): cluster that has been loaded

        @param kwargs: extra keyword arguments
        @type kwargs: L{dict}
        @return: the cluster that should be returned
        @rtype: L{pyzim.cluster.Cluster}
        """
        return kwargs["cluster"]

    def before_cluster_write(self, **kwargs):
        """
        Called before a cluster will be written.

        Keyword arguments:

            - C{cluster} (L{pyzim.cluster.Cluster}): cluster that should be written

        @param kwargs: extra keyword arguments
        @type kwargs: L{dict}
        @return: the cluster that should be written
        @rtype: L{pyzim.cluster.Cluster}
        """
        return kwargs["cluster"]

    def after_cluster_write(self, **kwargs):
        """
        Called after a cluster has been written.

        Keyword arguments:

            - C{cluster} (L{pyzim.cluster.Cluster}): cluster that should be written
            - C{old_offset} (L{int} or L{None}) offset the cluster had before
            - C{new_offset} (L{int}) offset the cluster has been written to
            - C{cluster_number} (L{int}) number of the cluster that has been written

        @param kwargs: extra keyword arguments
        @type kwargs: L{dict}
        """
        pass

    def before_entry_get(self, **kwargs):
        """
        Called when L{pyzim.archive.Zim.get_entry_at} was called.

        This is called at the beginning of said method.

        Keyword arguments:

            - C{location} (L{int}): location/offset of the entry to load
            - C{allow_cache_replacement} (L{int}): see L{pyzim.archive.Zim.get_entry_at}

        @param kwargs: extra keyword arguments
        @type kwargs: L{dict}
        """
        pass

    def after_entry_get(self, **kwargs):
        """
        Called when L{pyzim.archive.Zim.get_entry_at} was called, before the entry is returned.

        Keyword arguments:

            - C{location} (L{int}): location/offset of the entry to load
            - C{entry} (L{pyzim.entry.BaseEntry}): entry that should be returned
            - C{allow_cache_replacement} (L{int}): see L{pyzim.archive.Zim.get_entry_at}

        @param kwargs: extra keyword arguments
        @type kwargs: L{dict}
        @return: the entry that should be returned
        @rtype: L{pyzim.entry.BaseEntry}
        """
        return kwargs["entry"]

    def before_entry_write(self, **kwargs):
        """
        Called before an entry will be written.

        Keyword arguments:

            - C{entry} (L{pyzim.entry.BaseEntry}): entry that should be written
            - C{add_to_title_pointer_list} (L{bool}): See L{pyzim.archive.Zim.write_entry}
            - C{update_redirects} (L{bool}): See L{pyzim.archive.Zim.write_entry}

        @param kwargs: extra keyword arguments
        @type kwargs: L{dict}
        @return: the entry that should be written
        @rtype: L{pyzim.entry.BaseEntry}
        """
        return kwargs["entry"]

    def after_entry_write(self, **kwargs):
        """
        Called after an entry was written.

        Keyword arguments:

            - C{entry} (L{pyzim.entry.BaseEntry}): entry that should be written
            - C{old_entry} (L{pyzim.entry.BaseEntry} or L{None}): previous, unmodified entry, if any
            - C{old_offset} (L{int} or L{None}): offset of old entry, if any
            - C{new_offset} (L{int}): offset of new entry
            - C{is_new_entry} (L{bool}): whether this is a new entry or not
            - C{add_to_title_pointer_list} (L{bool}): See L{pyzim.archive.Zim.write_entry}
            - C{update_redirects} (L{bool}): See L{pyzim.archive.Zim.write_entry}

        @param kwargs: extra keyword arguments
        @type kwargs: L{dict}
        """
        return kwargs["entry"]

    def before_entry_remove(self, **kwargs):
        """
        Called before an entry will be removed.

        Keyword arguments:

            - C{full_url} (L{str}): full url of entry to remove
            - C{blob} (L{str}): See L{pyzim.archive.Zim.remove_entry_by_full_url}

        @param kwargs: extra keyword arguments
        @type kwargs: L{dict}
        """
        pass

    def after_entry_remove(self, **kwargs):
        """
        Called after an entry has been removed.

        Keyword arguments:

            - C{full_url} (L{str}): full url of entry to remove
            - C{blob} (L{str}): See L{pyzim.archive.Zim.remove_entry_by_full_url}
            - C{entry} (L{pyzim.entry.BaseEntry}): entry that has been removed
            - C{is_article} (L{bool}): whether the removed entry was an article or not.

        @param kwargs: extra keyword arguments
        @type kwargs: L{dict}
        """
        pass

    def before_flush(self, **kwargs):
        """
        Called before the archive will be flushed.

        @param kwargs: extra keyword arguments
        @type kwargs: L{dict}
        """
        pass

    def after_content_flush(self, **kwargs):
        """
        Called during flush, after all content has been flushed.

        At this point, the various pointerlists may not have yet been flushed.

        @param kwargs: extra keyword arguments
        @type kwargs: L{dict}
        """
        pass

    def after_flush(self, **kwargs):
        """
        Called after the archive has been flushed.

        @param kwargs: extra keyword arguments
        @type kwargs: L{dict}
        """
        pass
