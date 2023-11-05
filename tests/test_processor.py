"""
Tests for L{pyzim.processor}.
"""
import unittest

from pyzim import constants
from pyzim.processor import BaseProcessor
from pyzim.archive import Zim
from pyzim.entry import BaseEntry, RedirectEntry
from pyzim.cluster import Cluster

from .base import TestBase


class ProcessorHelper(BaseProcessor):
    """
    Test processor implementation
    """
    def __init__(self):
        BaseProcessor.__init__(self)
        self.called = {}  # fname -> called (bool)

    def on_install(self, zim, **kwargs):
        assert isinstance(zim, Zim)
        assert not self.called.get("on_install", False)
        assert not self.called.get("before_close", False)
        assert not self.called.get("after_close", False)
        self.zim = zim
        self.called["on_install"] = True

    def before_close(self, **kwargs):
        assert self.called.get("on_install", False)
        assert not self.called.get("after_close", False)
        self.called["before_close"] = True

    def after_close(self, **kwargs):
        assert self.called.get("on_install", False)
        assert self.called.get("before_close", False)
        assert not self.called.get("after_close", False)
        assert self.called.get("after_flush", False)
        self.called["after_close"] = True

    def on_add_redirect(self, **kwargs):
        assert self.called.get("on_install", False)
        assert not self.called.get("after_close", False)
        self.called["on_add_redirect"] = True
        assert isinstance(kwargs["entry"], RedirectEntry)

    def before_cluster_get(self, **kwargs):
        assert self.called.get("on_install", False)
        assert not self.called.get("after_close", False)
        assert isinstance(kwargs["location"], int) and kwargs["location"] > 0
        self.called["before_get_cluster"] = True

    def after_cluster_get(self, **kwargs):
        assert self.called.get("on_install", False)
        assert not self.called.get("after_close", False)
        assert self.called.get("before_get_cluster", False)
        assert isinstance(kwargs["cluster"], Cluster)
        self.called["after_get_cluster"] = True
        return kwargs["cluster"]

    def before_cluster_write(self, **kwargs):
        assert self.called.get("on_install", False)
        assert not self.called.get("after_close", False)
        assert isinstance(kwargs["cluster"], Cluster)
        self.called["before_cluster_write"] = True
        return kwargs["cluster"]

    def after_cluster_write(self, **kwargs):
        assert self.called.get("on_install", False)
        assert not self.called.get("after_close", False)
        assert self.called.get("before_cluster_write", False)
        assert isinstance(kwargs["cluster"], Cluster)
        assert (kwargs["old_offset"] is None) or (isinstance(kwargs["old_offset"], int) and kwargs["old_offset"] > 0)
        assert isinstance(kwargs["new_offset"], int) and kwargs["new_offset"] > 0
        assert isinstance(kwargs["cluster_number"], int) and kwargs["cluster_number"] >= 0
        assert self.zim.get_cluster_index_by_offset(kwargs["new_offset"]) == kwargs["cluster_number"]
        self.called["after_cluster_write"] = True
        return kwargs["cluster"]

    def before_entry_get(self, **kwargs):
        assert self.called.get("on_install", False)
        assert not self.called.get("after_close", False)
        assert isinstance(kwargs["location"], int) and (kwargs["location"] > 0)
        assert isinstance(kwargs["allow_cache_replacement"], bool)
        self.called["before_entry_get"] = True

    def after_entry_get(self, **kwargs):
        assert self.called.get("on_install", False)
        assert not self.called.get("after_close", False)
        assert self.called.get("before_entry_get", False)
        assert isinstance(kwargs["entry"], BaseEntry)
        assert isinstance(kwargs["location"], int) and (kwargs["location"] > 0)
        assert isinstance(kwargs["allow_cache_replacement"], bool)
        self.called["after_entry_get"] = True
        return kwargs["entry"]

    def before_entry_write(self, **kwargs):
        assert self.called.get("on_install", False)
        assert not self.called.get("after_close", False)
        assert isinstance(kwargs["entry"], BaseEntry)
        assert isinstance(kwargs["add_to_title_pointer_list"], bool)
        assert isinstance(kwargs["update_redirects"], bool)
        self.called["before_entry_write"] = True
        return kwargs["entry"]

    def after_entry_write(self, **kwargs):
        assert self.called.get("on_install", False)
        assert not self.called.get("after_close", False)
        assert self.called.get("before_entry_write", False)
        assert isinstance(kwargs["entry"], BaseEntry)
        assert (kwargs["old_entry"] is None) or (isinstance(kwargs["old_entry"], BaseEntry) and kwargs["old_entry"] is not kwargs["entry"])
        assert (kwargs["old_offset"] is None) or (isinstance(kwargs["old_offset"], int) and kwargs["old_offset"] > 0)
        assert isinstance(kwargs["new_offset"], int) and kwargs["new_offset"] > 0
        assert isinstance(kwargs["add_to_title_pointer_list"], bool)
        assert isinstance(kwargs["update_redirects"], bool)
        self.called["after_entry_write"] = True

    def before_entry_remove(self, **kwargs):
        assert self.called.get("on_install", False)
        assert not self.called.get("after_close", False)
        assert isinstance(kwargs["full_url"], str) and (len(kwargs["full_url"]) > 0)
        assert isinstance(kwargs["blob"], str) and (kwargs["blob"] in ("keep", "empty", "remove"))
        self.called["before_entry_remove"] = True

    def after_entry_remove(self, **kwargs):
        assert self.called.get("on_install", False)
        assert not self.called.get("after_close", False)
        assert self.called.get("before_entry_remove")
        assert isinstance(kwargs["entry"], BaseEntry)
        assert isinstance(kwargs["is_article"], bool)
        assert isinstance(kwargs["full_url"], str) and (len(kwargs["full_url"]) > 0)
        assert isinstance(kwargs["blob"], str) and (kwargs["blob"] in ("keep", "empty", "remove"))
        self.called["after_entry_remove"] = True

    def before_flush(self):
        assert self.called.get("on_install", False)
        assert not self.called.get("after_close", False)
        self.called["before_flush"] = True

    def after_content_flush(self):
        assert self.called.get("on_install", False)
        assert not self.called.get("after_close", False)
        assert self.called.get("before_flush", False)
        assert not self.zim.compression_strategy.has_items()
        assert not self.zim.uncompressed_compression_strategy.has_items()
        self.called["after_content_flush"] = True

    def after_flush(self):
        assert self.called.get("on_install", False)
        assert not self.called.get("after_close", False)
        assert self.called.get("before_flush", False)
        assert self.called.get("after_content_flush", False)
        self.called["after_flush"] = True


class ProcessorTests(unittest.TestCase, TestBase):
    """
    Tests for L{pyzim.processor.BaseProcessor}.
    """
    def test_default(self):
        """
        Test that the default processor methods do not cause any issues.
        """
        with self.open_temp_dir() as zimdir:
            with zimdir.open(mode="w") as zim:
                p = BaseProcessor()
                zim.install_processor(p)
                self.populate_zim(zim)
                zim.flush()
                # check content
                # home.txt
                home_entry = zim.get_entry_by_url("C", "home.txt")
                self.assertEqual(home_entry.namespace, "C")
                self.assertEqual(home_entry.url, "home.txt")
                self.assertEqual(home_entry.title, "Welcome!")
                self.assertEqual(home_entry.mimetype, "text/plain")
                self.assertEqual(home_entry.read().decode(constants.ENCODING), "This is the mainpage.")
                # /sub/directory.txt
                subdirectory_entry = zim.get_entry_by_url("C", "/sub/directory.txt")
                self.assertEqual(subdirectory_entry.namespace, "C")
                self.assertEqual(subdirectory_entry.url, "/sub/directory.txt")
                self.assertEqual(subdirectory_entry.title, "Subdirectory")
                self.assertEqual(subdirectory_entry.mimetype, "text/plain")
                self.assertEqual(subdirectory_entry.read().decode(constants.ENCODING), "subdirectory_content")
                # markdown.md
                markdown_entry = zim.get_entry_by_url("C", "markdown.md")
                self.assertEqual(markdown_entry.namespace, "C")
                self.assertEqual(markdown_entry.url, "markdown.md")
                self.assertEqual(markdown_entry.title, "Markdown")
                self.assertEqual(markdown_entry.mimetype, "text/markdown")
                self.assertEqual(markdown_entry.read().decode(constants.ENCODING), "#Markdown Test")
                # namespace.txt
                namespace_entry = zim.get_entry_by_url("T", "namespace.txt")
                self.assertEqual(namespace_entry.namespace, "T")
                self.assertEqual(namespace_entry.url, "namespace.txt")
                self.assertEqual(namespace_entry.title, "Namespace")
                self.assertEqual(namespace_entry.mimetype, "text/plain")
                self.assertEqual(namespace_entry.read().decode(constants.ENCODING), "Namespace test")
                # hidden.txt
                hidden_entry = zim.get_entry_by_url("C", "hidden.txt")
                self.assertEqual(hidden_entry.namespace, "C")
                self.assertEqual(hidden_entry.url, "hidden.txt")
                self.assertEqual(hidden_entry.title, "Hidden")
                self.assertEqual(hidden_entry.mimetype, "text/plain")
                self.assertEqual(hidden_entry.read().decode(constants.ENCODING), "hidden content")
                # mainpage test
                mainpage_entry = zim.get_mainpage_entry().resolve()
                self.assertEqual(mainpage_entry.url, home_entry.url)
                self.assertEqual(mainpage_entry.mimetype, home_entry.mimetype)
                self.assertEqual(mainpage_entry.read(), home_entry.read())
            # validate ZIM
            if self.has_zimcheck():
                self.run_zimcheck(zimdir.get_full_path())

    def test_processor_helper(self):
        """
        Test processing using a helper processor.
        """
        with self.open_temp_dir() as zimdir:
            with zimdir.open(mode="w") as zim:
                p = ProcessorHelper()
                zim.install_processor(p)
                self.populate_zim(zim)
                zim.flush()
                # check content
                # home.txt
                home_entry = zim.get_entry_by_url("C", "home.txt")
                self.assertEqual(home_entry.namespace, "C")
                self.assertEqual(home_entry.url, "home.txt")
                self.assertEqual(home_entry.title, "Welcome!")
                self.assertEqual(home_entry.mimetype, "text/plain")
                self.assertEqual(home_entry.read().decode(constants.ENCODING), "This is the mainpage.")
                # /sub/directory.txt
                subdirectory_entry = zim.get_entry_by_url("C", "/sub/directory.txt")
                self.assertEqual(subdirectory_entry.namespace, "C")
                self.assertEqual(subdirectory_entry.url, "/sub/directory.txt")
                self.assertEqual(subdirectory_entry.title, "Subdirectory")
                self.assertEqual(subdirectory_entry.mimetype, "text/plain")
                self.assertEqual(subdirectory_entry.read().decode(constants.ENCODING), "subdirectory_content")
                # markdown.md
                markdown_entry = zim.get_entry_by_url("C", "markdown.md")
                self.assertEqual(markdown_entry.namespace, "C")
                self.assertEqual(markdown_entry.url, "markdown.md")
                self.assertEqual(markdown_entry.title, "Markdown")
                self.assertEqual(markdown_entry.mimetype, "text/markdown")
                self.assertEqual(markdown_entry.read().decode(constants.ENCODING), "#Markdown Test")
                # namespace.txt
                namespace_entry = zim.get_entry_by_url("T", "namespace.txt")
                self.assertEqual(namespace_entry.namespace, "T")
                self.assertEqual(namespace_entry.url, "namespace.txt")
                self.assertEqual(namespace_entry.title, "Namespace")
                self.assertEqual(namespace_entry.mimetype, "text/plain")
                self.assertEqual(namespace_entry.read().decode(constants.ENCODING), "Namespace test")
                # hidden.txt
                hidden_entry = zim.get_entry_by_url("C", "hidden.txt")
                self.assertEqual(hidden_entry.namespace, "C")
                self.assertEqual(hidden_entry.url, "hidden.txt")
                self.assertEqual(hidden_entry.title, "Hidden")
                self.assertEqual(hidden_entry.mimetype, "text/plain")
                self.assertEqual(hidden_entry.read().decode(constants.ENCODING), "hidden content")
                # mainpage test
                mainpage_entry = zim.get_mainpage_entry().resolve()
                self.assertEqual(mainpage_entry.url, home_entry.url)
                self.assertEqual(mainpage_entry.mimetype, home_entry.mimetype)
                self.assertEqual(mainpage_entry.read(), home_entry.read())
            # check called status
            expected_called = [
                "on_install",
                "before_close",
                "after_close",
                "on_add_redirect",
                "before_get_cluster",
                "after_get_cluster",
                "before_cluster_write",
                "after_cluster_write",
                "before_entry_write",
                "after_entry_write",
                "before_entry_remove",
                "after_entry_remove",
                "before_entry_get",
                "after_entry_get",
                "before_flush",
                "after_content_flush",
                "after_flush",
            ]
            for fname in expected_called:
                self.assertTrue(p.called.get(fname, False))
            # validate ZIM
            if self.has_zimcheck():
                self.run_zimcheck(zimdir.get_full_path())
