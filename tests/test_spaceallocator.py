"""
Tests for L{pyzim.spaceallocator}.
"""
import unittest

from pyzim.spaceallocator import SpaceAllocator

from .base import TestBase


class SpaceAllocatorTests(unittest.TestCase, TestBase):
    """
    Tests for L{pyzim.spaceallocator.SpaceAllocator}.
    """
    def test_mark_free_simple(self):
        """
        Test marking an area as free without any complex situations.
        """
        sa = SpaceAllocator(file_end=32)
        self.assertEqual(sa.free_blocks, [])
        sa.mark_free(2, 10)
        self.assertEqual(sa.free_blocks, [(2, 10)])
        sa.mark_free(16, 16)
        self.assertIn((2, 10), sa.free_blocks)
        self.assertIn((16, 16), sa.free_blocks)

    def test_mark_free_merge(self):
        """
        Test marking an area as free while merging previously free sections.
        """
        sa = SpaceAllocator(file_end=128)
        sa.mark_free(0, 2)
        sa.mark_free(64, 2)

        sa.mark_free(11, 6)
        sa.mark_free(21, 4)
        self.assertEqual(len(sa.free_blocks), 4)
        self.assertIn((0, 2), sa.free_blocks)
        self.assertIn((64, 2), sa.free_blocks)
        self.assertIn((11, 6), sa.free_blocks)
        self.assertIn((21, 4), sa.free_blocks)
        # merge previous sections
        sa.mark_free(17, 4)
        self.assertEqual(len(sa.free_blocks), 3)
        self.assertIn((0, 2), sa.free_blocks)
        self.assertIn((64, 2), sa.free_blocks)
        self.assertIn((11, 14), sa.free_blocks)
        # merge a section that follows
        sa.mark_free(25, 8)
        self.assertEqual(len(sa.free_blocks), 3)
        self.assertIn((0, 2), sa.free_blocks)
        self.assertIn((64, 2), sa.free_blocks)
        self.assertIn((11, 22), sa.free_blocks)
        # merge a section that starts prior
        sa.mark_free(9, 2)
        self.assertEqual(len(sa.free_blocks), 3)
        self.assertIn((0, 2), sa.free_blocks)
        self.assertIn((64, 2), sa.free_blocks)
        self.assertIn((9, 24), sa.free_blocks)
        # mark prior section with overlap
        sa.print_status()  # DEBUG
        sa.mark_free(5, 16)
        self.assertEqual(len(sa.free_blocks), 3)
        self.assertIn((0, 2), sa.free_blocks)
        self.assertIn((64, 2), sa.free_blocks)
        self.assertIn((5, 28), sa.free_blocks)
        # mark following section with overlap
        sa.mark_free(29, 8)
        self.assertEqual(len(sa.free_blocks), 3)
        self.assertIn((0, 2), sa.free_blocks)
        self.assertIn((64, 2), sa.free_blocks)
        self.assertIn((5, 32), sa.free_blocks)
        # test that marking with len 0 does not change anything
        sa.mark_free(37, 0)
        self.assertEqual(len(sa.free_blocks), 3)
        self.assertIn((0, 2), sa.free_blocks)
        self.assertIn((64, 2), sa.free_blocks)
        self.assertIn((5, 32), sa.free_blocks)

    def test_allocate(self):
        """
        Test L{pyzim.spaceallocator.SpaceAllocator.allocate}.
        """
        file_end = 32
        sa = SpaceAllocator(
            [
                (4, 4),
                (10, 8),
                (29, 3),
            ],
            file_end=file_end,
        )
        self.assertEqual(len(sa.free_blocks), 3)
        location = sa.allocate(6)
        self.assertEqual(location, 10)
        self.assertEqual(len(sa.free_blocks), 3)
        location = sa.allocate(2)
        self.assertEqual(location, 16)
        self.assertEqual(len(sa.free_blocks), 2)
        location = sa.allocate(4)
        self.assertEqual(location, 4)
        self.assertEqual(len(sa.free_blocks), 1)
        # allocate at block before file end
        location = sa.allocate(8)
        self.assertEqual(location, 29)
        self.assertEqual(len(sa.free_blocks), 0)
        # require append at file end
        location = sa.allocate(16)
        self.assertEqual(location, 37)
        self.assertEqual(len(sa.free_blocks), 0)
        self.assertEqual(sa.file_end, 37 + 16)
        # test allocate 0 -> file end
        location = sa.allocate(0)
        self.assertEqual(location, sa.file_end)
