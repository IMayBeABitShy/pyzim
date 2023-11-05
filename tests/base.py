"""
Base module for tests.
"""
import contextlib
import os
import shutil
import tempfile
import subprocess

from pyzim.archive import Zim
from pyzim import blob, item


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

    @cvar TEST_ZIM_META: metadata for standard test zim file
    @type TEST_ZIM_META: L{dict} of L{str} -> L{str}
    @cvar NUM_ENTRIES: expected number of entries in the standard test ZIM file
    @type NUM_ENTRIES: L{int}
    """

    TEST_ZIM_META = {
            "Name": "testzim",
            "Title": "Test Zim",
            "Creator": "pyzim",
            "Publisher": "pyzim",
            "Date": "2023-09-11",
            "Description": "A ZIM file for testing",
            "Language": "Eng",
            "Illustration_48x48@1": "some placeholder data",
        }
    NUM_ENTRIES = 5 + 2 + 1 + len(TEST_ZIM_META) + 1  # 5 items, 2 title lists, 1 redirect, 1 counter

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

    def has_zimcheck(self):
        """
        Check if the 'zimcheck' tool is installed.

        @return: True if zimcheck is installed
        @rtype: L{bool}
        """
        try:
            subprocess.check_call(["zimcheck", "--version"])
        except subprocess.CalledProcessError:
            # zimcheck exited with error code 0
            return False
        else:
            return True

    def run_zimcheck(self, path):
        """
        Run 'zimcheck' on the specified zim.

        @param path: path of zim file to run zimcheck on
        @type path: L{str}
        """
        subprocess.check_call(["zimcheck", "--all", "--details", path], stderr=subprocess.STDOUT)

    def add_item(self, zim, namespace, url, title, mimetype, content, is_article=False):
        """
        Helper function for quickly adding an entry to the zim archive.

        @param zim: zim archive to add item to
        @type zim: L{pyzim.archive.Zim}
        @param namespace: namespace of entry
        @type namespace: L{str}
        @param url: url of entry
        @type url: L{str} or L{bytes}
        @param title: title of entry
        @type title: L{str}
        @param mimetype: mimetype of entry
        @type mimetype: L{str}
        @param content: content of the blob
        @type content: L{str} or L{bytes}
        @param is_article: whether the item should be an article or not
        @type is_article: L{bool}
        """
        assert isinstance(zim, Zim)
        assert isinstance(namespace, str)
        assert isinstance(url, str)
        assert isinstance(title, str)
        assert isinstance(mimetype, str)
        assert isinstance(content, (str, bytes))

        content_blob = blob.InMemoryBlobSource(content)
        new_item = item.Item(
            namespace=namespace,
            url=url,
            mimetype=mimetype,
            title=title,
            blob_source=content_blob,
            is_article=is_article,
        )
        zim.add_item(new_item)

    def populate_zim(self, zim):
        """
        Populate an ZIM archive with some default content.

        @param zim: zim archive to populate
        @type zim: L{pyzim.archive.Zim}
        """
        self.add_item(
            zim,
            namespace="C",
            url="home.txt",
            title="Welcome!",
            mimetype="text/plain",
            content="This is the mainpage.",
            is_article=True,
        )
        self.add_item(
            zim,
            namespace="C",
            url="/sub/directory.txt",
            title="Subdirectory",
            mimetype="text/plain",
            content="subdirectory_content",
            is_article=True,
        )
        self.add_item(
            zim,
            namespace="C",
            url="markdown.md",
            title="Markdown",
            mimetype="text/markdown",
            content="#Markdown Test",
            is_article=True,
        )
        self.add_item(
            zim,
            namespace="C",
            url="hidden.txt",
            title="Hidden",
            mimetype="text/plain",
            content="hidden content",
            is_article=False,
        )
        self.add_item(
            zim,
            namespace="T",
            url="namespace.txt",
            title="Namespace",
            mimetype="text/plain",
            content="Namespace test",
            is_article=False,
        )
        zim.set_mainpage_url("home.txt")
        for k, v in self.TEST_ZIM_META.items():
            zim.set_metadata(k, v)
