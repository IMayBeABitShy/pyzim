"""
Base module for tests.
"""
import contextlib
import os
import shutil
import tempfile

from pyzim.archive import Zim


class TempZimDir(object):
    """
    A helper class that allows opening a ZIM multiple times in a temp dir.

    @ivar path: path to the temporary directory
    @type path: L{str}
    @ivar name: name of file to open by default
    @type name: L{str}
    """
    def __init__(self, path, name="test.zim"):
        """
        The default constructor.

        @param path: path to the temporary directory.
        @type path: L{str}
        @param name: name of file to open by default
        @type name: L{str}
        """
        assert isinstance(path, str)
        assert isinstance(name, str)
        self.path = path
        self.name = name

    def get_full_path(self, name=None):
        """
        Return the full path for the specified file name.

        @param name: name of file to return path for, defaulting to L{TempZimDir.name}.
        @type name: L{str} or L{None}
        @return: the full path of the specified file
        @rtype: L{str}
        """
        if name is None:
            name = self.name
        assert isinstance(name, str)
        return os.path.join(self.path, name)

    @contextlib.contextmanager
    def open(self, name=None, **kwargs):
        """
        Open a ZIM inside the temporary directory.

        This is a context-manager, that closes the zim automatically.

        @param name: name of file to open, defaulting to L{TempZimDir.name}.
        @type name: L{str} or L{None}
        @param kwargs: kwargs to pass to L{pyzim.archive.Zim.open}
        @return: a context manager providing the ZIM
        """
        fullpath = self.get_full_path(name)
        with Zim.open(fullpath, **kwargs) as zim:
            try:
                yield zim
            finally:
                zim.close()


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

    @contextlib.contextmanager
    def open_zts_small_dir(self):
        """
        Open a temporary directory with a copy of zts-small.

        @return: a context manager providing an interface for opening ZIM files inside the dir
        @rtype: L{TempZimDir}
        """
        orgpath = self.get_zts_small_path()
        with tempfile.TemporaryDirectory() as tempdir:
            tempzimpath = os.path.join(tempdir, "small.zim")
            shutil.copyfile(orgpath, tempzimpath)
            yield TempZimDir(path=tempdir, name="small.zim")

    @contextlib.contextmanager
    def open_temp_dir(self):
        """
        Open a temporary directory as a L{TempZimDir} in a context.

        @return: a context manager providing an empty L{TempZimDir}
        @rtype: context manager providing L{TempZimDir}
        """
        with tempfile.TemporaryDirectory() as tempdir:
            yield TempZimDir(path=tempdir)
