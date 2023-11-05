"""
Tests for L{pyzim.counter}.
"""
import unittest

from pyzim import constants, exceptions
from pyzim.counter import Counter

from .base import TestBase


class CounterTests(unittest.TestCase, TestBase):
    """
    Tests for L{pyzim.counter.Counter}.
    """
    def test_simple(self):
        """
        Test a simple usage of the counter.
        """
        c = Counter()
        c.increment_count("a")
        self.assertEqual(c.get_count("a"), 1)
        # should be 0 for unknown entries
        self.assertEqual(c.get_count("b"), 0)

    def test_increment_decrement(self):
        """
        Test L{pyzim.counter.Counter.increment_count} and L{pyzim.counter.Counter.decrement_count}.
        """
        c = Counter()
        c.increment_count("a")
        self.assertEqual(c.get_count("a"), 1)
        c.increment_count("a")
        self.assertEqual(c.get_count("a"), 2)
        c.increment_count("a")
        self.assertEqual(c.get_count("a"), 3)
        c.increment_count("b")
        self.assertEqual(c.get_count("b"), 1)
        c.decrement_count("a")
        self.assertEqual(c.get_count("a"), 2)
        # check for type error
        with self.assertRaises(TypeError):
            c.increment_count(1)
        with self.assertRaises(TypeError):
            c.decrement_count(1)
        # check for value error on invalid decrement
        c.decrement_count("b")
        with self.assertRaises(ValueError):
            c.decrement_count("b")
        with self.assertRaises(ValueError):
            c.decrement_count("c")

    def test_serialization(self):
        """
        Test the serialization of the counter.
        """
        # load from a string
        c = Counter.from_string("image/jpeg=5;image/gif=3;image/png=2")
        self.assertEqual(c.get_count("image/jpeg"), 5)
        self.assertEqual(c.get_count("image/gif"), 3)
        self.assertEqual(c.get_count("image/png"), 2)
        self.assertEqual(c.get_count("text/plain"), 0)
        # add and remove a value so we can be sure no empty mimetypes will be inserted.
        c.increment_count("foo")
        c.decrement_count("foo")
        # dump back to string
        s = c.to_string()
        self.assertIn("image/jpeg=5", s)
        self.assertIn("image/gif=3", s)
        self.assertIn("image/png=2", s)
        self.assertIn(";image", s)
        self.assertNotIn("foo", s)
        self.assertEqual(len(s), 36)
        # load string again
        c2 = Counter.from_string(s)
        self.assertEqual(c2.get_count("image/jpeg"), 5)
        self.assertEqual(c2.get_count("image/gif"), 3)
        self.assertEqual(c2.get_count("image/png"), 2)
        self.assertEqual(c2.get_count("text/plain"), 0)
        # test with encoded string
        c3 = Counter.from_string(s.encode(constants.ENCODING))
        self.assertEqual(c3.get_count("image/jpeg"), 5)
        self.assertEqual(c3.get_count("image/gif"), 3)
        self.assertEqual(c3.get_count("image/png"), 2)
        self.assertEqual(c3.get_count("text/plain"), 0)
        # check empty string load
        c4 = Counter.from_string("")
        self.assertEqual(c4.get_count("image/jpeg"), 0)
        self.assertEqual(c4.get_count("image/gif"), 0)
        self.assertEqual(c4.get_count("image/png"), 0)
        self.assertEqual(c4.get_count("text/plain"), 0)
        # ensure type error on invalid type
        with self.assertRaises(TypeError):
            Counter.from_string(1234)

    def test_get_count(self):
        """
        Test L{pyzim.counter.Counter.get_count}.
        """
        c = Counter()
        c.increment_count("a")
        self.assertEqual(c.get_count("a"), 1)
        # it should also work with undefined entries
        self.assertEqual(c.get_count("b"), 0)
        # it should also work if we decrement a back to 0 again
        c.decrement_count("a")
        self.assertEqual(c.get_count("a"), 0)
        # check for type error
        with self.assertRaises(TypeError):
            c.get_count(1234)

    def test_load_from_archive(self):
        """
        Test L{pyzim.counter.Counter.load_from_archive}.
        """
        # test with zts-small
        with self.open_zts_small() as zim:
            # check for type error
            with self.assertRaises(TypeError):
                Counter.load_from_archive(zim, how=1234)
            # check for value error
            with self.assertRaises(ValueError):
                Counter.load_from_archive(zim, how="make something up")
            # regular tests
            # "ignore"
            c = Counter.load_from_archive(zim, how="ignore")
            self.assertIsNone(c)
            # "load"
            c = Counter.load_from_archive(zim, how="load")
            self.assertIsNotNone(c)
            self.assertEqual(c.get_count("text/html"), 1)
            self.assertEqual(c.get_count("image/png"), 1)
            # "reinit"
            c = Counter.load_from_archive(zim, how="reinit")
            self.assertIsNotNone(c)
            self.assertEqual(c.get_count("text/html"), 1)
            self.assertEqual(c.get_count("image/png"), 1)
            # "load_or_reinit"
            c = Counter.load_from_archive(zim, how="load_or_reinit")
            self.assertIsNotNone(c)
            self.assertEqual(c.get_count("text/html"), 1)
            self.assertEqual(c.get_count("image/png"), 1)
        # test with a new, empty archive
        with self.open_temp_dir() as zimdir:
            # "ignore"
            with zimdir.open(mode="w") as zim:
                c = Counter.load_from_archive(zim, how="ignore")
                self.assertIsNone(c)
            # "load"
            with zimdir.open(mode="w") as zim:
                with self.assertRaises(exceptions.NoCounter):
                    c = Counter.load_from_archive(zim, how="load")
            # "reinit"
            with zimdir.open(mode="w") as zim:
                c = Counter.load_from_archive(zim, how="reinit")
                self.assertIsNotNone(c)
                self.assertEqual(c.get_count("text/html"), 0)
                self.assertEqual(c.get_count("image/png"), 0)
            # "load_or_reinit"
            with zimdir.open(mode="w") as zim:
                c = Counter.load_from_archive(zim, how="load_or_reinit")
                self.assertIsNotNone(c)
                self.assertEqual(c.get_count("text/html"), 0)
                self.assertEqual(c.get_count("image/png"), 0)

    def test_zim_creation(self):
        """
        Test the counter during ZIM creation.
        """
        with self.open_temp_dir() as zimdir:
            with zimdir.open(mode="w") as zim:
                self.assertEqual(zim.counter.get_count("text/plain"), 0)
                self.assertEqual(zim.counter.get_count("text/markdown"), 0)
                self.assertEqual(zim.counter.get_count("text/madeup"), 0)
                self.populate_zim(zim)
                zim.flush()
                self.assertEqual(zim.counter.get_count("text/plain"), 3)
                self.assertEqual(zim.counter.get_count("text/markdown"), 1)
                self.assertEqual(zim.counter.get_count("text/madeup"), 0)
            # re-open zim, testing that loading works
            with zimdir.open(mode="r") as zim:
                self.assertEqual(zim.counter.get_count("text/plain"), 3)
                self.assertEqual(zim.counter.get_count("text/markdown"), 1)
                self.assertEqual(zim.counter.get_count("text/madeup"), 0)

    def test_zim_edit(self):
        """
        Test the counter during ZIM editing.
        """
        with self.open_zts_small_dir() as zimdir:
            with zimdir.open(mode="u") as zim:
                self.assertEqual(zim.counter.get_count("text/html"), 1)
                self.assertEqual(zim.counter.get_count("image/png"), 1)
                # add an article, increasing the mimetype count
                self.add_item(
                    zim,
                    namespace="C",
                    url="test_file.txt",
                    title="Test",
                    mimetype="text/html",
                    content="This is the test file.",
                )
                zim.flush()
                self.assertEqual(zim.counter.get_count("text/html"), 2)
                self.assertEqual(zim.counter.get_count("image/png"), 1)
                # adding a non-c entry or a redirect should not change count
                zim.add_redirect("someredirect", "test_file.txt")
                self.add_item(
                    zim,
                    namespace="T",
                    url="test_file.txt",
                    title="Test",
                    mimetype="text/html",
                    content="This is another test file",
                )
                zim.flush()
                self.assertEqual(zim.counter.get_count("text/html"), 2)
                self.assertEqual(zim.counter.get_count("image/png"), 1)
                # removing a C entry should decrease mimetype count
                zim.remove_entry_by_full_url("Ctest_file.txt")
                zim.flush()
                self.assertEqual(zim.counter.get_count("text/html"), 1)
                self.assertEqual(zim.counter.get_count("image/png"), 1)
            # read zim to ensure counter was correctly stored
            with zimdir.open(mode="r") as zim:
                self.assertEqual(zim.counter.get_count("text/html"), 1)
                self.assertEqual(zim.counter.get_count("image/png"), 1)
            # validate zim
            if self.has_zimcheck():
                self.run_zimcheck(zimdir.get_full_path())
