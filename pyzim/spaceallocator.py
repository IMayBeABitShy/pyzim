"""
This module contains functionality for managing the space inside a file.

@var logger: logger used for logging messages of the spaceallocator.
@type logger: L{logging.Logger}
"""
from .constants import LOG_LEVEL_ALLOCATION

import threading
import logging

from .bindable import BindableMixIn


logger = logging.getLogger(__name__)


class SpaceAllocator(BindableMixIn):
    """
    The SpaceAllocator is used to manage the space inside a ZIM file.

    This means that the space allocator keeps track of "empty" sections
    in a ZIM file, so we can later efficently write data there.
    For example, when modifying an existing ZIM file, it's possible that
    an entry or a cluster becomes larger than it was before. If the ZIM
    file was previously using its size perfectly, that means we can't
    write the entry/cluster to the same location as there is not enough
    space there. Instead, a new location is needed, but this leaves the
    previous location "empty". We can later recycle this empty space and
    potentially write other entries/clusters there. This is the reason
    for the existence of this class.

    This class is implemented as a modified "Contigous Allocation"
    component. However, rather than having a fixed max size and keeping
    track of allocated blocks, we have an open-end and keep track of
    free blocks as a tuple of (offset, size).

    Additionally, this method also keeps track of the end of the file,
    so that we know where we can append data.

    @ivar free_blocks: a list of tuples of (offset, size) indicating free locations
    @type free_blocks: L{list} of L{tuple} of (L{int}, L{int})
    @ivar file_end: offset to the end of the file (first non-written byte)
    @type file_end: L{int}
    @ivar lock: thread-safety lock
    @type lock: L{threading.Lock}
    """
    def __init__(self, free_blocks=None, file_end=0):
        """
        The default constructor.

        @param free_blocks: initial free block positions, see class documentation
        @type free_blocks: L{list} of L{tuple} of (L{int}, L{int})
        @param file_end: initial end of file (index of first non-written byte)
        @type file_end: L{int}
        """
        assert isinstance(free_blocks, list) or free_blocks is None
        assert isinstance(file_end, int)
        if free_blocks is None:
            self.free_blocks = []
        else:
            self.free_blocks = free_blocks[:]
        self.file_end = file_end
        self.lock = threading.Lock()

    def allocate(self, block_size):
        """
        Allocate some space inside the file and return an offset at which
        this data can be safely written.

        This method tries to find the smallest free block of sufficient
        size. If none is found, storage space will be appended at the
        file end, reusing a potential free block directly prior to the
        file end.

        @param block_size: number of bytes to allocate
        @type block_size: L{int}
        @return: an offset at which the specified number of bytes can be written
        @rtype: L{int}
        """
        assert isinstance(block_size, int) and block_size >= 0
        # special case: if 0 bytes need to be written (unlikely, more likely a bug!), we could
        # return any index
        if block_size == 0:
            logger.warning("SpaceAllocator was requested to allocate 0 bytes, this is likely a bug!")
            return self.file_end

        # we are modifying the internal list, so acquire lock
        with self.lock:
            # locate smallest free block of sufficient size
            min_size = None
            best_index = None
            for index, (start, length) in enumerate(self.free_blocks):
                if length >= block_size and (min_size is None or length < min_size):
                    min_size = length
                    best_index = index

            if best_index is not None:
                # a free block has been found
                start, length = self.free_blocks[best_index]
                if length > block_size:
                    # reduce remaining free block size
                    new_start = start + block_size
                    self.free_blocks[best_index] = (new_start, length - block_size)
                else:
                    # remove block
                    self.free_blocks.pop(best_index)
                logger.log(LOG_LEVEL_ALLOCATION, "Allocated {} bytes in free block at {}, leaving {} bytes free".format(block_size, start, length - block_size))
                return start

            # no free block of sufficient size found
            trailing_free_index = len(self.free_blocks) - 1
            if trailing_free_index >= 0:
                # we have at least one free block
                trailing_free_block = self.free_blocks[trailing_free_index]
                if trailing_free_block[0] + trailing_free_block[1] == self.file_end:
                    # there is a free block directly prior to the file end
                    # use this one up first
                    start, length = self.free_blocks[trailing_free_index]
                    self.free_blocks.pop(trailing_free_index)
                    extra_bytes_needed = max(block_size - length, 0)
                    self.file_end += extra_bytes_needed
                    logger.log(LOG_LEVEL_ALLOCATION, "Allocated {} bytes at file end at {}, recycling {} bytes from a free trailing block".format(block_size, start, length))
                    return start

            # append at file end
            new_start = self.file_end
            self.file_end += block_size
            logger.log(LOG_LEVEL_ALLOCATION, "Allocated {} bytes at file end at {}".format(block_size, new_start))
            return new_start

    def mark_free(self, start, length):
        """
        Mark a section in the file as free.

        @param start: offset to section to mark as free
        @type start: L{int}
        @param length: number of bytes to mark as free
        @type length: L{int}
        """
        assert isinstance(start, int) and start >= 0
        assert isinstance(length, int) and length >= 0

        logger.log(LOG_LEVEL_ALLOCATION, "Marking {} bytes at {} as free".format(length, start))

        if length == 0:
            # nothing to mark
            return

        # we are modifying the internal list of free blocks, so acquire lock
        with self.lock:
            # find insertion point (bisect will likely be uncecessary)
            # also use this opportunity to copy the list
            new_blocks = []
            insert_index = 0
            for i, block in enumerate(self.free_blocks):
                new_blocks.append(block)
                block_start, block_length = block
                if block_start <= start:
                    insert_index = i + 1
            # insert new block
            new_blocks.insert(insert_index, (start, length))

            # join adjacent and overlapping blocks
            # we do this by always comparing it to the previous block
            prev_block = None
            i = 0
            while i < len(new_blocks):
                block = new_blocks[i]
                block_start, block_length = block
                block_end = block_start + block_length
                if i == 0:
                    # first block won't be merged
                    i += 1
                    prev_block = block
                    continue
                # check if block is adjacent or overlapping
                prev_block_end = prev_block[0] + prev_block[1]
                if prev_block_end >= block_start:
                    # adjacent, merge
                    merged_end = max(prev_block_end, block_end)
                    merged_block_length = merged_end - prev_block[0]
                    merged_block = (prev_block[0], merged_block_length)
                    # overwrite previous block and remove current block
                    new_blocks[i-1] = prev_block = merged_block
                    del new_blocks[i]
                else:
                    # non-adjacent
                    prev_block = block
                    i += 1

            self.free_blocks = new_blocks

    def print_status(self):
        """
        Print the current allocation status and details of free blocks.
        """
        print("File end: {}".format(self.file_end))
        print("Storage blocks:")
        for start, length in self.free_blocks:
            print("  Start: {}, Length: {}".format(start, length))
        print("\n")


if __name__ == "__main__":  # pragma: no cover
    # Example usage
    allocator = SpaceAllocator()

    allocator.print_storage_status()
    print("Allocating block of size 20")
    start1 = allocator.allocate(20)
    allocator.print_storage_status()
    print("Allocating block of size 30")
    start2 = allocator.allocate(30)
    allocator.print_storage_status()
    print("Marking block as free:", start1, "with length 20")
    allocator.mark_free(start1, 20)
    allocator.print_storage_status()
    print("Allocating block of size 25")
    start3 = allocator.allocate(25)
    allocator.print_storage_status()
