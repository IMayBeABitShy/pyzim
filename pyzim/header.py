"""
This module contains the class for working with ZIM headers.
"""
import struct
import uuid as pyuuid  # so we can use "uuid" as a variable

from . import constants
from .exceptions import NotAZimFile, IncompatibleZimFile


class Header(object):
    """
    The header of a ZIM file.

    The header contains various metadata informations about the ZIM file
    as well as the offsets of some other datastructures in the ZIM file.

    @cvar MAGIC_NUMBER: the magic number used to identify ZIM files.
    @type MAGIC_NUMBER: L{int}
    @cvar FORMAT: the format of a ZIM header
    @type FORMAT: L{str}
    @cvar LENGTH: length of the ZIM header
    @type LENGTH: L{int}

    @ivar magic_number: magic number of this ZIM file. If set to None (recommended), use default.
    @type magic_number: L{int}
    @ivar major_version: the major version of this ZIM file
    @type major_version : L{int}
    @ivar minor_version: the minor version of this ZIM file
    @type minor_version: L{int}
    @ivar uuid: uuid of the ZIM file
    @type uuid: L{uuid.UUID}
    @ivar entry_count: number of entries in the archive
    @type entry_count: L{int}
    @ivar cluster_count: number of clusters in the archive
    @type cluster_count: L{int}
    @ivar url_pointer_position: offset to the directory pointer list ordered by URL
    @type url_pointer_position: L{int}
    @ivar title_pointer_position: offset tot he directory pointer list ordered by title
    @type title_pointer_position: L{int}
    @ivar cluster_pointer_position: offset to the cluster pointer list
    @type cluster_pointer_position: L{int}
    @ivar mime_list_position: offset to the mime type list
    @type mime_list_position: L{int}
    @ivar main_page: index of the main page in the url pointer list, 0xffffffff if not present
    @type main_page: L{int}
    @ivar layout_page: index of the main page in the url pointer list, 0xffffffff if not present
    @type layout_page: L{int}
    @ivar checksum_position: offset to the checksum
    @type checksum_position: L{int}
    """
    MAGIC_NUMBER = 72173914
    FORMAT = constants.ENDIAN + "IHH16sIIQQQQIIQ"
    LENGTH = struct.calcsize(FORMAT)

    def __init__(
        self,
        magic_number,
        major_version,
        minor_version,
        uuid,
        entry_count,
        cluster_count,
        url_pointer_position,
        title_pointer_position,
        cluster_pointer_position,
        mime_list_position,
        main_page,
        layout_page,
        checksum_position,
    ):
        """
        The default constructor.

        The arguments are arranged in the same order as they appear in the ZIM header.
        This allows directly unpacking the header string into this constructor.

        @param magic_number: magic number of this ZIM file. If set to None (recommended), use default.
        @type magic_number: L{int}
        @param major_version: the major version of this ZIM file
        @type major_version : L{int}
        @param minor_version: the minor version of this ZIM file
        @type minor_version: L{int}
        @param uuid: uuid of the ZIM file
        @type uuid: L{int} or L{bytes} or L{uuid.UUID}
        @param entry_count: number of entries in the archive
        @type entry_count: L{int}
        @param cluster_count: number of clusters in the archive
        @type cluster_count: L{int}
        @param url_pointer_position: offset to the directory pointer list ordered by URL
        @type url_pointer_position: L{int}
        @param title_pointer_position: offset tot he directory pointer list ordered by title
        @type title_pointer_position: L{int}
        @param cluster_pointer_position: offset to the cluster pointer list
        @type cluster_pointer_position: L{int}
        @param mime_list_position: offset to the mime type list
        @type mime_list_position: L{int}
        @param main_page: index of the main page in the url pointer list, 0xffffffff if not present
        @type main_page: L{int}
        @param layout_page: index of the main page in the url pointer list, 0xffffffff if not present
        @type layout_page: L{int}
        @param checksum_position: offset to the checksum
        @type checksum_position: L{int}
        """
        if magic_number is None:
            magic_number = self.MAGIC_NUMBER

        assert isinstance(magic_number, int) or (magic_number is None)
        assert isinstance(major_version, int) and major_version >= 0
        assert isinstance(minor_version, int) and minor_version >= 0
        assert isinstance(uuid, (int, bytes, pyuuid.UUID))
        assert isinstance(entry_count, int) and entry_count >= 0
        assert isinstance(cluster_count, int) and cluster_count >= 0
        assert isinstance(url_pointer_position, int) and url_pointer_position >= 0
        assert isinstance(title_pointer_position, int) and title_pointer_position >= 0
        assert isinstance(cluster_pointer_position, int) and cluster_pointer_position >= 0
        assert isinstance(mime_list_position, int) and mime_list_position >= 0
        assert isinstance(main_page, int) and main_page >= 0
        assert isinstance(layout_page, int) and layout_page >= 0
        assert isinstance(checksum_position, int) and checksum_position >= 0
        self.magic_number = magic_number
        self.major_version = major_version
        self.minor_version = minor_version
        if isinstance(uuid, int):
            self.uuid = pyuuid.UUID(int=uuid)
        elif isinstance(uuid, bytes):
            self.uuid = pyuuid.UUID(bytes_le=uuid)
        elif isinstance(uuid, pyuuid.UUID):
            self.uuid = uuid
        else:
            raise TypeError("uuid must be either bytes, int or a uuid.UUID, not {!r}!".format(type(uuid)))
        self.entry_count = entry_count
        self.cluster_count = cluster_count
        self.url_pointer_position = url_pointer_position
        self.title_pointer_position = title_pointer_position
        self.cluster_pointer_position = cluster_pointer_position
        self.mime_list_position = mime_list_position
        self.main_page = main_page
        self.layout_page = layout_page
        self.checksum_position = checksum_position

    def __str__(self):
        """
        Return a string displaying the content of the header.

        @return: a string describing the header
        @rtype: L{str}
        """
        template = """
Magic number: {mn}
Version: {mjv} (major) / {mnv} (minor)
UUID: {uuid}
Content: {ne} entries, {nc} clusters
Offsets:
    Directory pointer list: {upl}
    Cluster pointer list: {cpl}
    Mime type list: {mtl}
    Checksum: {cs}
Pages:
    Main: {mp}
    Layout: {lp}
        """
        filled = template.format(
            mn=self.magic_number,
            mjv=self.major_version,
            mnv=self.minor_version,
            uuid=self.uuid,
            ne=self.entry_count,
            nc=self.cluster_count,
            upl=self.url_pointer_position,
            cpl=self.cluster_pointer_position,
            mtl=self.mime_list_position,
            cs=self.checksum_position,
            mp=(self.main_page if self.has_main_page else "none"),
            lp=(self.layout_page if self.has_layout_page else "none"),
        )
        return filled

    def check_compatible(self):
        """
        Check if this header is compatible, raising an exception if not.

        @raises pyzim.exceptions.NotAZimFile: when the provided file is not a ZIM file.
        @raises pyzim.exceptions.IncompatibleZimFile: when the ZIM file is not compatible
        """
        if self.magic_number != self.MAGIC_NUMBER:
            raise NotAZimFile(
                "Magic number of ZIM archives should be {}, but header appears to contain magic number {}!".format(
                    self.MAGIC_NUMBER, self.magic_number,
                )
            )
        if self.major_version not in constants.COMPATIBLE_ZIM_VERSIONS:
            raise IncompatibleZimFile("Zim version {} not supported!".format(self.major_version))
        if self.minor_version != 1:
            raise IncompatibleZimFile("PyZim currently only supports the new ZIM namespace format")

    @property
    def has_main_page(self):
        """
        A boolean indicating whether this header knows the position of the main page or not.

        @return: True if the mainPage pointer is set, False otherwise
        @rtype: L{bool}
        """
        return self.main_page != 0xffffffff

    @property
    def has_layout_page(self):
        """
        A boolean indicating whether this header knows the position of the layout page or not.

        @return: True if the layoutPage pointer is set, False otherwise
        @rtype: L{bool}
        """
        return self.layout_page != 0xffffffff

    @classmethod
    def from_bytes(cls, s):
        """
        Construct a header from a bytestring.

        @param s: string to parse
        @type s: L{bytes}
        @return: the header parsed from the bytes
        @rtype: L{pyzim.header.Header}
        """
        assert isinstance(s, bytes)
        if len(s) != cls.LENGTH:
            # invalid header length
            raise ValueError("Header length must be {}, got {}!".format(cls.LENGTH, len(s)))
        values = struct.unpack(cls.FORMAT, s)
        return cls(*values)

    @classmethod
    def from_file(cls, f, seek=None):
        """
        Read the header from a file.

        @param f: file-like object to read from
        @type f: file-like
        @param seek: if specified, seek this position of the file before reading
        @type seek: L{int} or L{None}
        @return: the header read from the file
        @rtype: L{pyzim.header.Header}
        """
        assert (isinstance(seek, int) and seek >= 0) or seek is None
        if seek is not None:
            f.seek(seek)
        data = f.read(cls.LENGTH)
        return cls.from_bytes(data)

    def to_bytes(self):
        """
        Dump this header into a bytestring.

        @return: a bytestring representation of this header.
        @rtype: L{bytes}
        """
        packed = struct.pack(
            self.FORMAT,  # format to dump as
            self.magic_number,
            self.major_version,
            self.minor_version,
            self.uuid.bytes_le,
            self.entry_count,
            self.cluster_count,
            self.url_pointer_position,
            self.title_pointer_position,
            self.cluster_pointer_position,
            self.mime_list_position,
            self.main_page,
            self.layout_page,
            self.checksum_position,
        )
        return packed

    def to_dict(self):
        """
        Dump this header into a dictionary and return it.

        Some values may be converted.

        @return: a dictionary containing the information in this header
        @rtype: L{dict}
        """
        data = {
            "magic_number:": self.magic_number,
            "version_major": self.major_version,
            "version_minor": self.minor_version,
            "uuid": self.uuid.hex,
            "uuid_int": self.uuid.int,
            "count_entries": self.entry_count,
            "count_clusters": self.cluster_count,
            "pointer_url": self.url_pointer_position,
            "pointer_title": self.title_pointer_position,
            "pointer_cluster": self.cluster_pointer_position,
            "pointer_mimetype": self.mime_list_position,
            "has_main_page": self.has_main_page,
            "index_main_page": self.main_page,
            "has_layout_page": self.has_layout_page,
            "index_layout_page": self.layout_page,
            "pointer_checksum": self.checksum_position,
        }
        return data


if __name__ == "__main__":  # pragma: no cover
    # test script
    import argparse
    parser = argparse.ArgumentParser(description="Print the header of a ZIM file")
    parser.add_argument("path", help="path to file to read")
    ns = parser.parse_args()

    with open(ns.path, "rb") as fin:
        header = Header.from_file(fin)
        print(str(header))
