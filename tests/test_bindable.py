"""
Tests for L{pyzim.bindable}.
"""
import unittest

from pyzim.exceptions import AlreadyBound
from pyzim.bindable import BindableMixIn

from .base import TestBase


class SomeBindable(BindableMixIn):
    """
    A subclass of BindableMixIn for tests.
    """
    def __init__(self, zim=None):
        BindableMixIn.__init__(self, zim=zim)


class BindableTests(unittest.TestCase, TestBase):
    """
    Tests for L{pyzim.bindable.BindableMixIn} and its subclasses.
    """
    def test_init(self):
        """
        Test all __init__ behavior.
        """
        with self.open_zts_small() as zim:
            bindable_1 = SomeBindable()
            self.assertFalse(bindable_1.bound)
            self.assertIsNone(bindable_1.zim)
            bindable_2 = SomeBindable(zim=zim)
            self.assertTrue(bindable_2.bound)
            self.assertIsNotNone(bindable_2.zim)

    def test_readonly_attributes(self):
        """
        Test that some attributes are read-only.
        """
        with self.open_zts_small() as zim:
            bindable_1 = SomeBindable()
            self.assertIsNone(bindable_1.zim)
            with self.assertRaises(AttributeError):
                bindable_1.zim = zim
            self.assertIsNone(bindable_1.zim)
            with self.assertRaises(AttributeError):
                bindable_1.bound = True
            self.assertFalse(bindable_1.bound)

            bindable_2 = SomeBindable(zim=zim)
            self.assertIsNotNone(bindable_2.zim)
            with self.assertRaises(AttributeError):
                bindable_2.zim = None
            self.assertIsNotNone(bindable_2.zim)
            with self.assertRaises(AttributeError):
                bindable_2.bound = False
            self.assertTrue(bindable_2.bound)

    def test_bind(self):
        """
        Test binding and unbinding behavior.
        """
        with self.open_zts_small() as zim:
            bindable = SomeBindable()
            self.assertFalse(bindable.bound)
            self.assertIsNone(bindable.zim)
            # bind
            bindable.bind(zim)
            self.assertTrue(bindable.bound)
            self.assertIsNotNone(bindable.zim)
            self.assertIs(bindable.zim, zim)
            # try to bind again to the same zim
            bindable.bind(zim)
            self.assertTrue(bindable.bound)
            self.assertIsNotNone(bindable.zim)
            self.assertIs(bindable.zim, zim)
            # try to bind again to a different zim
            with self.open_zts_small() as zim_2:
                with self.assertRaises(AlreadyBound):
                    bindable.bind(zim_2)
            self.assertTrue(bindable.bound)
            self.assertIsNotNone(bindable.zim)
            self.assertIs(bindable.zim, zim)
            # unbind again
            bindable.unbind()
            self.assertFalse(bindable.bound)
            self.assertIsNone(bindable.zim)
