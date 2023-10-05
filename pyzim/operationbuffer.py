"""
This module contains the L{pyzim.operationbuffer.OperationBuffer} class.
"""
from pyzim.exceptions import UnresolvedRedirect


class OperationBuffer(object):
    """
    The OperationBuffer is responsible for buffering ZIM write operations until they can be completed.

    Due to the way compression works, it is possible that some operations
    like L{pyzim.archive.Zim.add_full_url_redirect} can only be completed
    after a new cluster has been written. As force-flushing the
    compression strategy every time this happens would lead to
    potentially suboptimal compression, we have to buffer these
    operations until they can be completed. This class is responsible
    for this.

    @ivar zim: ZIM archiveto buffer operations for
    @type zim: L{pyzim.archive.Zim}
    @ivar _buffered_redirects: a list of (source full url, target full url, title) of redirects that need yet to be written
    @type _buffered_redirects: L{list} of L{tuple} of (L{str}, L{str}, L{str} or L{None})
    @ivar _buffered_mainpage_url: the buffered value for the next mainpage url setting (or L{False} if none buffered)
    @type _buffered_mainpage_url: L{str} or L{None} or L{False}
    """
    def __init__(self, zim):
        """
        The default constructor.

        @param zim: ZIM archiveto buffer operations for
        @type zim: L{pyzim.archive.Zim}
        """
        self.zim = zim
        self._buffered_redirects = []
        self._buffered_mainpage_url = False  # None has another meaning here

    def finalize_entry_dependent_operations(self):
        """
        Finalize all operations that require other entries to be written first.

        NOTE: currently, access to the title pointer list entries is not available.

        @raises pyzim.exceptions.UnresolvedRedirect: when a redirect target could not be resolved
        """
        self._apply_redirects()
        self._apply_set_mainpage_url()

    def buffer_add_redirect(self, source, target, title=None):
        """
        Buffer an add_redirect operation.

        @param source: source full url of the redirect
        @type source: L{str}
        @param target: target full url of the redirect
        @type target: L{str}
        @param title: title of redirect to add
        @type title: L{str} or L{None}
        """
        assert isinstance(source, str)
        assert isinstance(target, str)
        assert isinstance(title, str) or (title is None)
        self._buffered_redirects.append((source, target, title))

    def _apply_redirects(self):
        """
        Apply previously buffered add_redirect operations.

        @raises pyzim.exceptions.UnresolvedRedirect: when a redirect target can not be resolved
        """
        # there is an important special case:
        # it is possible that some redirects can only be resolved when
        # some other redirects that were added later were written
        # thus, we re-buffer these entries and repeatedly try to write
        # them until all redirects are written or we can detect no
        # changes, in which case an exception will be raised.
        buffer_changed = True
        while buffer_changed:
            redirects_to_write = self._buffered_redirects
            self._buffered_redirects = []
            for source_url, target_url, title in redirects_to_write:
                self.zim.add_full_url_redirect(source_url, target_url, title=title)
            buffer_changed = (self._buffered_redirects != redirects_to_write)
        # check if there are any redirects remaining
        num_unresolved = len(self._buffered_redirects)
        if num_unresolved > 0:
            raise UnresolvedRedirect("Could not resolve {} redirects: {}".format(num_unresolved, self._buffered_redirects))

    def buffer_set_mainpage_url(self, url):
        """
        Buffer a set_mainpage_url operation.

        See L{pyzim.archive.Zim.set_mainpage_url} for details.

        Any such previously buffered operations are replaced.

        @param url: non-full url of mainpage to set (or L{None} to disable mainapge)
        @type url: L{str} or L{None}
        """
        assert isinstance(url, str) or (url is None)
        self._buffered_mainpage_url = url

    def _apply_set_mainpage_url(self):
        """
        Apply any previously buffered set_mainpage_url operation.

        @raises pyzim.exceptions.UnresolvedRedirect: If the redirect for the mainpage can not be resolved.
        """
        if self._buffered_mainpage_url is False:
            # no operation buffered
            return
        url = self._buffered_mainpage_url
        self._buffered_mainpage_url = False
        self.zim.set_mainpage_url(url)
        # check if this operation got buffered again.
        # NOTE: to clear any potentially buffered values, setting set
        # mainpage URL to None will result in an additional buffering of
        # the value None. This value must be ignored in the check.
        if (self._buffered_mainpage_url is not False) and (self._buffered_mainpage_url is not None):
            raise UnresolvedRedirect("Could not resolved mainpage url redirect: {}".format(url))
