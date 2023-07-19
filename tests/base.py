"""
Base module for tests.
"""
import contextlib
import os

from pyzim.archive import Zim


class TestBase(object):
    """
    A mix-in class for pyzim tests, providing shared functionality.
    """
    def get_zts_small_path(self):
        """
        Return the path of small.zim from the ZTS.

        @return: the header
        @rtype: L{pyzim.header.Header}
        """
        path = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            "zim-testing-suite/data/nons/small.zim",
        )
        return path

    @contextlib.contextmanager
    def open_zts_small(self, **kwargs):
        """
        Return a ZIM archive for small.zim from the ZTS.

        @param kwargs: keyword arguments to pass to L{pyzim.archive.Zim.open}
        @return: a contextmanager providing the ZIM archive
        @rtype: L{pyzim.archive.Zim}
        """
        path = self.get_zts_small_path()
        with Zim.open(path, mode="r", **kwargs) as archive:
            try:
                yield archive
            finally:
                archive.close()
