"""
This module contains the class for working with ZIM headers.
"""
import struct
import uuid as pyuuid  # so we can use "uuid" as a variable

from . import constants
from .modifiable import ModifiableMixIn
from .exceptions import NotAZimFile, IncompatibleZimFile


class Header(ModifiableMixIn):
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

    @ivar _magic_number: magic number of this ZIM file.
    @type _magic_number: L{int}
    @ivar _major_version: the major version of this ZIM file
    @type _major_version : L{int}
    @ivar _minor_version: the minor version of this ZIM file
    @type _minor_version: L{int}
    @ivar _uuid: uuid of the ZIM file
    @type _uuid: L{uuid.UUID}
    @ivar _entry_count: number of entries in the archive
    @type _entry_count: L{int}
    @ivar _cluster_count: number of clusters in the archive
    @type _cluster_count: L{int}
    @ivar _url_pointer_position: offset to the directory pointer list ordered by URL
    @type _url_pointer_position: L{int}
    @ivar _title_pointer_position: offset tot he directory pointer list ordered by title
    @type _title_pointer_position: L{int}
    @ivar _cluster_pointer_position: offset to the cluster pointer list
    @type _cluster_pointer_position: L{int}
    @ivar _mime_list_position: offset to the mime type list
    @type _mime_list_position: L{int}
    @ivar _main_page: index of the main page in the url pointer list, 0xffffffff if not present
    @type _main_page: L{int}
    @ivar _layout_page: index of the layout page in the url pointer list, 0xffffffff if not present
    @type _layout_page: L{int}
    @ivar _checksum_position: offset to the checksum
    @type _checksum_position: L{int}
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
        @param layout_page: index of the layout page in the url pointer list, 0xffffffff if not present
        @type layout_page: L{int}
        @param checksum_position: offset to the checksum
        @type checksum_position: L{int}
        """
        ModifiableMixIn.__init__(self)

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
        self._magic_number = magic_number
        self._major_version = major_version
        self._minor_version = minor_version
        if isinstance(uuid, int):
            self._uuid = pyuuid.UUID(int=uuid)
        elif isinstance(uuid, bytes):
            self._uuid = pyuuid.UUID(bytes_le=uuid)
        elif isinstance(uuid, pyuuid.UUID):
            self._uuid = uuid
        else:
            raise TypeError("uuid must be either bytes, int or a uuid.UUID, not {!r}!".format(type(uuid)))
        self._entry_count = entry_count
        self._cluster_count = cluster_count
        self._url_pointer_position = url_pointer_position
        self._title_pointer_position = title_pointer_position
        self._cluster_pointer_position = cluster_pointer_position
        self._mime_list_position = mime_list_position
        self._main_page = main_page
        self._layout_page = layout_page
        self._checksum_position = checksum_position

        # ensure we know the current object size before modifications later
        self.after_flush_or_read()

    @classmethod
    def placeholder(cls):
        """
        Generate a placeholder header and return it.

        A placeholder header is a header populated with some reasonable
        defaults as well as placeholder values. It is not usable for an
        actual ZIM file, but has the correct length. This makes it useful
        as part of the ZIM creation process.

        @return: a placeholder header
        @rtype: L{Header}
        """
        header = cls(
            magic_number=cls.MAGIC_NUMBER,
            major_version=constants.ZIM_MAJOR_VERSION,
            minor_version=constants.ZIM_MINOR_VERSION,
            uuid=pyuuid.uuid4(),
            entry_count=0,
            cluster_count=0,
            url_pointer_position=0,
            title_pointer_position=0,
            cluster_pointer_position=0,
            mime_list_position=0,
            main_page=0xffffffff,
            layout_page=0xffffffff,
            checksum_position=0,
        )
        header.mark_dirty()
        return header

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
    Title pointer list: {tpl}
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
            tpl=self.title_pointer_position,
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
        if self.minor_version != constants.ZIM_MINOR_VERSION:
            raise IncompatibleZimFile("PyZim currently only supports the new ZIM namespace format")

    # =============== properties ===============
    # the following set of functions may seem rather long, but in actuality
    # they do mostly the same, just for different attributes
    # These functions are properties whose main task is to track whether
    # the value has been modified, so we know if we have to re-write
    # the header
    #
    # Each of the properties behave as following:
    # getter:
    #   1. return the value of the attribute
    #   1.1. some values may be converted (e.g. main_page -> None) if not set
    # setter:
    #   1. check if type and value are valid
    #   2. sometimes, the value may be converted
    #   3. ensure the header is allowed be be modified
    #   4. if the value differs, change the value and mark header as dirty

    @property
    def magic_number(self):
        """
        The magic number of this ZIM file.

        @return: the magic number of this ZIM file.
        @rtype: L{int}
        """
        return self._magic_number

    @magic_number.setter
    def magic_number(self, value):
        """
        The magic number of this ZIM file.

        @param value: value to set
        @type value: L{int}
        @raises TypeError: if value is not an integer
        @raises pyzim.exceptions.NonMutable: if this header is set to be inmutable.
        """
        if not isinstance(value, int):
            raise TypeError("Expected int, got {}".format(type(value)))
        self.ensure_mutable()
        if value != self._magic_number:
            self._magic_number = value
            self.mark_dirty()

    @property
    def major_version(self):
        """
        The major version of this ZIM file.

        @return: the major version of this ZIM file.
        @rtype: L{int}
        """
        return self._major_version

    @major_version.setter
    def major_version(self, value):
        """
        The major version of this ZIM file.

        @param value: value to set
        @type value: L{int}
        @raises TypeError: if value is not an integer
        @raises pyzim.exceptions.NonMutable: if this header is set to be inmutable.
        @raises ValueError: if the value is invalid (e.g. negative).
        """
        if not isinstance(value, int):
            raise TypeError("Expected int, got {}".format(type(value)))
        if value < 0:
            raise ValueError("Value can not be negative!")
        self.ensure_mutable()
        if value != self._major_version:
            self._major_version = value
            self.mark_dirty()

    @property
    def minor_version(self):
        """
        The minor version of this ZIM file.

        @return: the minor version of this ZIM file.
        @rtype: L{int}
        """
        return self._minor_version

    @minor_version.setter
    def minor_version(self, value):
        """
        The minor version of this ZIM file.

        @param value: value to set
        @type value: L{int}
        @raises TypeError: if value is not an integer
        @raises pyzim.exceptions.NonMutable: if this header is set to be inmutable.
        @raises ValueError: if the value is invalid (e.g. negative).
        """
        if not isinstance(value, int):
            raise TypeError("Expected int, got {}".format(type(value)))
        if value < 0:
            raise ValueError("Value can not be negative!")
        self.ensure_mutable()
        if value != self._minor_version:
            self._minor_version = value
            self.mark_dirty()

    @property
    def uuid(self):
        """
        The UUID of this ZIM file.

        @return: the UUID of this ZIM file.
        @rtype: L{uuid.UUID}
        """
        return self._uuid

    @uuid.setter
    def uuid(self, value):
        """
        The UUID of this ZIM file.

        @param value: value to set
        @type value: L{int} or L{bytes} or L{uuid.UUID}
        @raises TypeError: if value is not an integer
        @raises pyzim.exceptions.NonMutable: if this header is set to be inmutable.
        """
        if isinstance(value, int):
            converted = pyuuid.UUID(int=value)
        elif isinstance(value, bytes):
            converted = pyuuid.UUID(bytes_le=value)
        elif isinstance(value, pyuuid.UUID):
            converted = value
        else:
            raise TypeError("uuid must be either bytes, int or a uuid.UUID, not {!r}!".format(type(value)))
        self.ensure_mutable()
        if converted != self._uuid:
            self._uuid = converted
            self.mark_dirty()

    @property
    def entry_count(self):
        """
        The number of entries in this archive.

        @return: The number of entries in this archive.
        @rtype: L{int}
        """
        return self._entry_count

    @entry_count.setter
    def entry_count(self, value):
        """
        The number of entries in this archive.

        @param value: value to set
        @type value: L{int}
        @raises TypeError: if value is not an integer
        @raises pyzim.exceptions.NonMutable: if this header is set to be inmutable.
        @raises ValueError: if the value is invalid (e.g. negative)
        """
        if not isinstance(value, int):
            raise TypeError("Expected int, got {}".format(type(value)))
        if value < 0:
            raise ValueError("Value can not be negative!")
        self.ensure_mutable()
        if value != self._entry_count:
            self._entry_count = value
            self.mark_dirty()

    @property
    def cluster_count(self):
        """
        The number of clusters in this archive.

        @return: The number of clusters in this archive.
        @rtype: L{int}
        """
        return self._cluster_count

    @cluster_count.setter
    def cluster_count(self, value):
        """
        The number of clusters in this archive.

        @param value: value to set
        @type value: L{int}
        @raises TypeError: if value is not an integer
        @raises pyzim.exceptions.NonMutable: if this header is set to be inmutable.
        @raises ValueError: if the value is invalid (e.g. negative)
        """
        if not isinstance(value, int):
            raise TypeError("Expected int, got {}".format(type(value)))
        if value < 0:
            raise ValueError("Value can not be negative!")
        self.ensure_mutable()
        if value != self._cluster_count:
            self._cluster_count = value
            self.mark_dirty()

    @property
    def url_pointer_position(self):
        """
        The offset to the directory pointer list ordered by URL.

        @return: The offset to the directory pointer list ordered by URL.
        @rtype: L{int}
        """
        return self._url_pointer_position

    @url_pointer_position.setter
    def url_pointer_position(self, value):
        """
        The offset to the directory pointer list ordered by URL.

        @param value: value to set
        @type value: L{int}
        @raises TypeError: if value is not an integer
        @raises pyzim.exceptions.NonMutable: if this header is set to be inmutable.
        @raises ValueError: if the value is invalid (e.g. negative)
        """
        if not isinstance(value, int):
            raise TypeError("Expected int, got {}".format(type(value)))
        if value < 0:
            raise ValueError("Value can not be negative!")
        self.ensure_mutable()
        if value != self._url_pointer_position:
            self._url_pointer_position = value
            self.mark_dirty()

    @property
    def title_pointer_position(self):
        """
        The offset to the directory pointer list ordered by title.

        @return: The offset to the directory pointer list ordered by title.
        @rtype: L{int}
        """
        return self._title_pointer_position

    @title_pointer_position.setter
    def title_pointer_position(self, value):
        """
        The offset to the directory pointer list ordered by title.

        @param value: value to set
        @type value: L{int}
        @raises TypeError: if value is not an integer
        @raises pyzim.exceptions.NonMutable: if this header is set to be inmutable.
        @raises ValueError: if the value is invalid (e.g. negative)
        """
        if not isinstance(value, int):
            raise TypeError("Expected int, got {}".format(type(value)))
        if value < 0:
            raise ValueError("Value can not be negative!")
        self.ensure_mutable()
        if value != self._title_pointer_position:
            self._title_pointer_position = value
            self.mark_dirty()

    @property
    def cluster_pointer_position(self):
        """
        The offset to the cluster pointer list.

        @return: The offset to the cluster pointer list.
        @rtype: L{int}
        """
        return self._cluster_pointer_position

    @cluster_pointer_position.setter
    def cluster_pointer_position(self, value):
        """
        The offset to the cluster pointer list.

        @param value: value to set
        @type value: L{int}
        @raises TypeError: if value is not an integer
        @raises pyzim.exceptions.NonMutable: if this header is set to be inmutable.
        @raises ValueError: if the value is invalid (e.g. negative)
        """
        if not isinstance(value, int):
            raise TypeError("Expected int, got {}".format(type(value)))
        if value < 0:
            raise ValueError("Value can not be negative!")
        self.ensure_mutable()
        if value != self._cluster_pointer_position:
            self._cluster_pointer_position = value
            self.mark_dirty()

    @property
    def mime_list_position(self):
        """
        The offset to the mime type list.

        @return: The offset to the mime type list.
        @rtype: L{int}
        """
        return self._mime_list_position

    @mime_list_position.setter
    def mime_list_position(self, value):
        """
        The offset to the mime type list.

        @param value: value to set
        @type value: L{int}
        @raises TypeError: if value is not an integer
        @raises pyzim.exceptions.NonMutable: if this header is set to be inmutable.
        @raises ValueError: if the value is invalid (e.g. negative)
        """
        if not isinstance(value, int):
            raise TypeError("Expected int, got {}".format(type(value)))
        if value < 0:
            raise ValueError("Value can not be negative!")
        self.ensure_mutable()
        if value != self._mime_list_position:
            self._mime_list_position = value
            self.mark_dirty()

    @property
    def main_page(self):
        """
        The index of the main page in the url pointer list, L{None} if not present.

        @return: The index of the main page in the url pointer list, L{None} if not present.
        @rtype: L{int} or L{None}
        """
        if self._main_page == 0xffffffff:
            return None
        return self._main_page

    @main_page.setter
    def main_page(self, value):
        """
        The index of the main page in the url pointer list, L{None} if not present.

        @param value: value to set
        @type value: L{int} or L{None}
        @raises TypeError: if value is not an integer nor L{None}
        @raises pyzim.exceptions.NonMutable: if this header is set to be inmutable.
        @raises ValueError: if the value is invalid (e.g. negative)
        """
        if not isinstance(value, int) and (value is not None):
            raise TypeError("Expected int or None, got {}".format(type(value)))
        if (value is not None) and (value < 0):
            raise ValueError("Value can not be negative!")
        self.ensure_mutable()
        if value is None:
            value = 0xffffffff
        if value != self._main_page:
            self._main_page = value
            self.mark_dirty()

    @property
    def has_main_page(self):
        """
        A boolean indicating whether this header knows the position of the main page or not.

        @return: True if the mainPage pointer is set, False otherwise
        @rtype: L{bool}
        """
        return self.main_page is not None

    @property
    def layout_page(self):
        """
        The index of the layout page in the url pointer list, L{None} if not present.

        @return: The index of the layout page in the url pointer list, L{None} if not present.
        @rtype: L{int} or L{None}
        """
        if self._layout_page == 0xffffffff:
            return None
        return self._layout_page

    @layout_page.setter
    def layout_page(self, value):
        """
        The index of the layout page in the url pointer list, L{None} if not present.

        @param value: value to set
        @type value: L{int} or L{None}
        @raises TypeError: if value is not an integer nor L{None}
        @raises pyzim.exceptions.NonMutable: if this header is set to be inmutable.
        @raises ValueError: if the value is invalid (e.g. negative)
        """
        if not isinstance(value, int) and (value is not None):
            raise TypeError("Expected int or None, got {}".format(type(value)))
        if (value is not None) and (value < 0):
            raise ValueError("Value can not be negative!")
        self.ensure_mutable()
        if value is None:
            value = 0xffffffff
        if value != self._layout_page:
            self._layout_page = value
            self.mark_dirty()

    @property
    def has_layout_page(self):
        """
        A boolean indicating whether this header knows the position of the layout page or not.

        @return: True if the layoutPage pointer is set, False otherwise
        @rtype: L{bool}
        """
        return self.layout_page is not None

    @property
    def checksum_position(self):
        """
        The offset to the checksum.

        @return: The offset to the checksum.
        @rtype: L{int}
        """
        return self._checksum_position

    @checksum_position.setter
    def checksum_position(self, value):
        """
        The offset to the checksum.

        @param value: value to set
        @type value: L{int}
        @raises TypeError: if value is not an integer
        @raises pyzim.exceptions.NonMutable: if this header is set to be inmutable.
        @raises ValueError: if the value is invalid (e.g. negative)
        """
        if not isinstance(value, int):
            raise TypeError("Expected int, got {}".format(type(value)))
        if value < 0:
            raise ValueError("Value can not be negative!")
        self.ensure_mutable()
        if value != self._checksum_position:
            self._checksum_position = value
            self.mark_dirty()

    # ================ converters =====================

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
        header = cls(*values)
        return header

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
            self._magic_number,
            self._major_version,
            self._minor_version,
            self._uuid.bytes_le,
            self._entry_count,
            self._cluster_count,
            self._url_pointer_position,
            self._title_pointer_position,
            self._cluster_pointer_position,
            self._mime_list_position,
            self._main_page,
            self._layout_page,
            self._checksum_position,
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

    def get_disk_size(self):
        return self.LENGTH


if __name__ == "__main__":  # pragma: no cover
    # test script
    import argparse
    parser = argparse.ArgumentParser(description="Print the header of a ZIM file")
    parser.add_argument("path", help="path to file to read")
    ns = parser.parse_args()

    with open(ns.path, "rb") as fin:
        header = Header.from_file(fin)
        print(str(header))
