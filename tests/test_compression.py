"""
Tests for L{pyzim.compression}.
"""
import io
import unittest
import random

from pyzim import exceptions
from pyzim.compression import CompressionRegistry, CompressionType, CompressionTarget, DecompressingReader, PassthroughCompressionInterface

from .base import TestBase


class AnotherPassthroughCompressionInterface(PassthroughCompressionInterface):
    """
    A subclass of L{pyzim.compression.PassthroughCompressionInterface} for tests.

    This class matches its parent's class exactly. We really only need a
    second class.
    """
    pass


class CompressionTests(unittest.TestCase, TestBase):
    """
    Tests for L{pyzim.compression}.
    """
    def test_compression_registry_noinit(self):
        """
        Test that the L{pyzim.compression.CompressionRegistry} can not be instantiated.
        """
        with self.assertRaises(RuntimeError):
            CompressionRegistry()

    def test_compression_registry_register(self):
        """
        Test that L{pyzim.compression.CompressionRegistry} can work with multiple interfaces for the same L{pyzim.compression.CompressionType}.
        """
        # before test, find all known compression interfaces
        known_interfaces_before = {
            ct: len(list(CompressionRegistry.iter_for(ct)))
            for ct in CompressionType
        }

        # by default, the PasstroughCompressionInterface should have been registered
        self.assertTrue(CompressionRegistry.has(CompressionType.NONE))
        # unregister all interfaces for compression NONE
        CompressionRegistry.unregister(CompressionType.NONE, None)
        self.assertFalse(CompressionRegistry.has(CompressionType.NONE))
        self.assertEqual(len(list(CompressionRegistry.iter_for(CompressionType.NONE))), 0)
        # register two interfaces
        CompressionRegistry.register(CompressionType.NONE, PassthroughCompressionInterface)
        self.assertTrue(CompressionRegistry.has(CompressionType.NONE))
        self.assertEqual(len(list(CompressionRegistry.iter_for(CompressionType.NONE))), 1)
        CompressionRegistry.register(CompressionType.NONE, AnotherPassthroughCompressionInterface)
        self.assertTrue(CompressionRegistry.has(CompressionType.NONE))
        self.assertEqual(len(list(CompressionRegistry.iter_for(CompressionType.NONE))), 2)
        # unregister the new class
        CompressionRegistry.unregister(CompressionType.NONE, AnotherPassthroughCompressionInterface)
        self.assertTrue(CompressionRegistry.has(CompressionType.NONE))
        self.assertEqual(len(list(CompressionRegistry.iter_for(CompressionType.NONE))), 1)

        # ensure no permament changes have been made to known interfaces
        known_interfaces_after = {
            ct: len(list(CompressionRegistry.iter_for(ct)))
            for ct in CompressionType
        }
        self.assertDictEqual(known_interfaces_before, known_interfaces_after)

    def test_compression_registry_unknown(self):
        """
        Test that the correct exception is raised when no interface is known for a L{pyzim.compression.CompressionType}.
        """
        with self.assertRaises(exceptions.UnsupportedCompressionType):
            CompressionRegistry.get(CompressionType.UNKNOWN)

    def test_compression_interfaces(self):
        """
        Test compression and decompression of all compression interfaces.
        """
        raw_data = b"test hello world foo bar baz"
        for compression_type in CompressionType:
            for compression_target in CompressionTarget:
                for compression_interface in CompressionRegistry.iter_for(compression_type):
                    compressor = compression_interface.get_compressor(
                        {"general.target": compression_target},
                    )
                    compressed = compressor.compress(raw_data)
                    compressed += compressor.flush()
                    decompressor = compression_interface.get_decompressor()
                    decompressed = decompressor.decompress(compressed, max_length=10)
                    while (not decompressor.needs_input) and not (decompressor.eof):
                        decompressed += decompressor.decompress(b"")

                    self.assertEqual(decompressor.unused_data, b"")
                    self.assertEqual(decompressed, raw_data)

    def test_compression_interfaces_big(self):
        """
        Test compression and decompression of all compression interfaces with a lot of data.
        """
        raw_data = b"test hello world foo bar baz" * 1024 * 5
        for compression_type in CompressionType:
            for compression_interface in CompressionRegistry.iter_for(compression_type):
                compressor = compression_interface.get_compressor()
                compressed = compressor.compress(raw_data)
                compressed += compressor.flush()
                decompressor = compression_interface.get_decompressor()
                decompressed = b""
                read = 0
                running = True
                while running:
                    if decompressor.needs_input:
                        n = random.randint(40, 120)
                        n = min(n, len(compressed) - read)
                        data = compressed[read:read+n]
                        read += n
                    else:
                        data = b""
                    to_read = random.randint(16, 64)
                    new_data = decompressor.decompress(data, max_length=to_read)
                    n_remaining = len(compressed) - read
                    if decompressor.eof or (n_remaining == 0 and decompressor.needs_input):
                        assert running
                        running = False
                    decompressed += new_data
                    self.assertLessEqual(len(new_data), to_read)

                self.assertEqual(decompressor.unused_data, b"")
                self.assertEqual(decompressed, raw_data)

    def test_decompressing_reader_read(self):
        """
        Test L{pyzim.compression.DecompressingReader.read}.
        """
        raw_data = b"hello world!" * 256
        compression_type = CompressionType.LZMA2
        compression_interface = CompressionRegistry.get(compression_type)
        compressor = compression_interface.get_compressor()
        compressed = compressor.compress(raw_data)
        compressed += compressor.flush()
        org_compressed_length = len(compressed)
        compressed += b"extradata"
        compressed_f = io.BytesIO(compressed)
        decompressor = compression_interface.get_decompressor()
        reader = DecompressingReader(compressed_f, decompressor)
        data = b""
        n_to_read = 2
        while True:
            read = reader.read(n_to_read, extra_decompress=50)
            self.assertLessEqual(len(read), n_to_read)
            if not read:
                break
            data += read
        self.assertEqual(data, raw_data)
        self.assertEqual(reader.total_compressed_size, org_compressed_length)

    def test_decompressing_reader_skip(self):
        """
        Test L{pyzim.compression.DecompressingReader.skip}.
        """
        raw_data = b"hello world!"
        compression_type = CompressionType.LZMA2
        compression_interface = CompressionRegistry.get(compression_type)
        compressor = compression_interface.get_compressor()
        compressed = compressor.compress(raw_data)
        compressed += compressor.flush()
        compressed_f = io.BytesIO(compressed)
        decompressor = compression_interface.get_decompressor()
        reader = DecompressingReader(compressed_f, decompressor)
        reader.skip(5)
        data = b""
        while True:
            read = reader.read(2)
            self.assertLessEqual(len(read), 2)
            if not read:
                break
            data += read
        self.assertEqual(data, raw_data[5:])

    def test_decompressing_reader_skip_to(self):
        """
        Test L{pyzim.compression.DecompressingReader.skip_to}.
        """
        raw_data = b"hello world!"
        compression_type = CompressionType.LZMA2
        compression_interface = CompressionRegistry.get(compression_type)
        compressor = compression_interface.get_compressor()
        compressed = compressor.compress(raw_data)
        compressed += compressor.flush()
        compressed_f = io.BytesIO(compressed)
        decompressor = compression_interface.get_decompressor()
        reader = DecompressingReader(compressed_f, decompressor)
        reader.skip(1)
        reader.skip_to(5)
        data = b""
        while True:
            read = reader.read(2)
            self.assertLessEqual(len(read), 2)
            if not read:
                break
            data += read
        self.assertEqual(data, raw_data[5:])

        with self.assertRaises(IOError):
            reader.skip_to(2)

    def test_decompressing_reader_read_n(self):
        """
        Test L{pyzim.compression.DecompressingReader.read_n}.
        """
        raw_data = b"hello world!" * 32
        compression_type = CompressionType.LZMA2
        compression_interface = CompressionRegistry.get(compression_type)
        compressor = compression_interface.get_compressor()
        compressed = compressor.compress(raw_data)
        compressed += compressor.flush()
        compressed_f = io.BytesIO(compressed)
        decompressor = compression_interface.get_decompressor()
        reader = DecompressingReader(compressed_f, decompressor)
        data = reader.read_n(12)
        self.assertEqual(data, raw_data[:12])
        remaining_data = reader.read_n(10000)
        self.assertEqual(remaining_data, raw_data[12:])

    def test_decompressing_reader_iter_read_1(self):
        """
        Test L{pyzim.compression.DecompressingReader.iter_read}.
        """
        raw_chunk = b"hello world!"
        raw_data = raw_chunk * 32
        compression_type = CompressionType.LZMA2
        compression_interface = CompressionRegistry.get(compression_type)
        compressor = compression_interface.get_compressor()
        compressed = compressor.compress(raw_data)
        compressed += compressor.flush()
        compressed_f = io.BytesIO(compressed)
        decompressor = compression_interface.get_decompressor()
        reader = DecompressingReader(compressed_f, decompressor)
        data = b""
        for chunk in reader.iter_read(buffersize=12):
            self.assertNotEqual(chunk, b"")
            self.assertLessEqual(len(chunk), 12)
            data += chunk
        self.assertEqual(data, raw_data)

    def test_decompressing_reader_iter_read_2(self):
        """
        Test L{pyzim.compression.DecompressingReader.iter_read}.
        """
        raw_chunk = b"hello world!"
        raw_data = raw_chunk * 32
        compression_type = CompressionType.LZMA2
        compression_interface = CompressionRegistry.get(compression_type)
        compressor = compression_interface.get_compressor()
        compressed = compressor.compress(raw_data)
        compressed += compressor.flush()
        compressed_f = io.BytesIO(compressed)
        decompressor = compression_interface.get_decompressor()
        reader = DecompressingReader(compressed_f, decompressor)
        data = b""
        for chunk in reader.iter_read(n=24, buffersize=12):
            self.assertNotEqual(chunk, b"")
            self.assertLessEqual(len(chunk), 12)
            data += chunk
        self.assertEqual(data, raw_chunk * 2)
