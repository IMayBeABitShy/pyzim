
"""
Tests for L{pyzim.modifiable}.
"""
import unittest

from pyzim.modifiable import ModifiableMixIn

from .base import TestBase


class SomeModifiable(ModifiableMixIn):
    """
    A subclass of ModifableMixIn for tests.

    @ivar s: a string that is used for size estimates
    @type s: L{str}
    """
    def __init__(self, s=""):
        """
        The default constructor.

        @param s: initial value for L{SomeModifiable.s}
        @type s: L{str}
        """
        assert isinstance(s, str)
        ModifiableMixIn.__init__(self)
        self.s = s
        self.after_flush_or_read()

    def set_s(self, s):
        """
        Set the value for L{SomeModifiable.s}.
        """
        assert isinstance(s, str)
        self.dirty = (s != self.s)
        self.s = s

    def get_disk_size(self):
        return len(self.s)

    def get_initial_disk_size(self):
        return 0


class SomeOtherModifiable(ModifiableMixIn):
    """
    A subclass of modifiableMixIn for tests.
    """
    def __init__(self):
        """
        The default constructor.
        """
        ModifiableMixIn.__init__(self)


class ModifiableTests(unittest.TestCase, TestBase):
    """
    Tests for L{pyzim.modifiable.ModifiableMixIn} and its subclasses.
    """
    def test_simple(self):
        """
        Test simple usage of dirty states.
        """
        obj = SomeModifiable()
        self.assertFalse(obj.dirty)
        obj.dirty = True
        self.assertTrue(obj.dirty)
        obj.dirty = False
        self.assertFalse(obj.dirty)
        obj.mark_dirty()
        self.assertTrue(obj.dirty)

    def test_submodifiable(self):
        """
        Test usage of submodifiables.
        """
        root = SomeModifiable()
        child_a = SomeModifiable()
        child_b = SomeOtherModifiable()

        self.assertFalse(root.dirty)
        self.assertFalse(child_a.dirty)
        self.assertFalse(child_b.dirty)

        # test dirty state before it is set as a submodifiable
        child_a.dirty = True
        self.assertFalse(root.dirty)
        self.assertTrue(child_a.dirty)
        self.assertFalse(child_b.dirty)
        child_a.dirty = False

        # make a root -> child_a -> child_b relation
        root.add_submodifiable(child_a)
        child_a.add_submodifiable(child_b)
        # adding a child twice should not cause any problems
        root.add_submodifiable(child_a)

        # check that we get a TypeError on other values
        with self.assertRaises(TypeError):
            root.add_submodifiable(object())

        # ensure dirty is still False
        self.assertFalse(root.dirty)
        self.assertFalse(child_a.dirty)
        self.assertFalse(child_b.dirty)

        # check that dirty state is inherited
        child_a.mark_dirty()

        self.assertTrue(root.dirty)
        self.assertTrue(child_a.dirty)
        self.assertFalse(child_b.dirty)

        # check that unsetting dirty state works with parents
        child_a.dirty = False

        self.assertFalse(root.dirty)
        self.assertFalse(child_a.dirty)
        self.assertFalse(child_b.dirty)

        # check both root and children being dirty
        child_a.mark_dirty()
        root.mark_dirty()

        self.assertTrue(root.dirty)
        self.assertTrue(child_a.dirty)
        self.assertFalse(child_b.dirty)

        child_a.dirty = False

        self.assertTrue(root.dirty)
        self.assertFalse(child_a.dirty)
        self.assertFalse(child_b.dirty)

        # remove child
        root.dirty = False
        child_a.dirty = True
        self.assertTrue(root.dirty)
        root.remove_submodifiable(child_a)
        self.assertFalse(root.dirty)

        # ensure error when removing a modifiable that's not a child
        with self.assertRaises(ValueError):
            root.remove_submodifiable(child_a)
        with self.assertRaises(ValueError):
            root.remove_submodifiable(child_b)
        # also check for type error
        with self.assertRaises(TypeError):
            root.remove_submodifiable(object())

    def test_get_disk_size(self):
        """
        Test L{pyzim.modifiable.ModifiableMixIn.get_disk_size}.
        """
        m = SomeModifiable("test")
        self.assertEqual(m.get_disk_size(), 4)
        m = SomeOtherModifiable()
        with self.assertRaises(NotImplementedError):
            m.get_disk_size()

    def test_after_flush_or_read(self):
        """
        Test L{pyzim.modifiable.ModifiableMixIn.after_flush_or_read}.
        """
        m = SomeModifiable("test")
        self.assertFalse(m.dirty)
        self.assertEqual(m.get_disk_size(), 4)
        # the first time, the old size should be 0 as the test class is implemented in such a way
        self.assertEqual(m.get_unmodified_disk_size(), 0)
        # update old size
        m.after_flush_or_read()
        self.assertEqual(m.get_disk_size(), m.get_unmodified_disk_size())
        # change value, which sould not modify old disk size
        m.set_s("foo")
        self.assertTrue(m.dirty)
        self.assertEqual(m.get_disk_size(), 3)
        self.assertEqual(m.get_unmodified_disk_size(), 4)
        # "flush"
        m.after_flush_or_read()
        self.assertFalse(m.dirty)
        self.assertEqual(m.get_disk_size(), 3)
        self.assertEqual(m.get_disk_size(), m.get_unmodified_disk_size())
