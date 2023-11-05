"""
This module contains various constants.

@var ZIM_MAJOR_VERSION: current major version of the ZIM standard implemented
@type ZIM_MAJOR_VERSION: L{int}
@var ZIM_MINOR_VERSION: current minor version of the ZIM standard implemented
@type ZIM_MINOR_VERSION: L{int}
@var COMPATIBLE_ZIM_VERSIONS: a tuple describing the compatible ZIM major versions
@type COMPATIBLE_ZIM_VERSIONS: L{tuple} of L{int}
@var ENCODING: (default) encoding to use
@type ENCODING: L{str}
@var ENDIAN: a string describing the endian-ness of integers in the ZIM file, see L{struct}
@type ENDIAN: L{str}
@var URL_ENTRY_TITLE_INDEX: full URL to the v0 title index of all entries
@type URL_ENTRY_TITLE_INDEX: L{str}
@var URL_ARTICLE_TITLE_INDEX: full URL to the v1 title index of all articles
@type URL_ARTICLE_TITLE_INDEX: L{str}
@var MIMETYPE_ZIMLISTING: the mimetype used by zim listings (e.g. the title indexes)
@type MIMETYPE_ZIMLISTING: L{str}
@var MIMETYPE_REDIRECT: symbolic mimetype used by internal redirects
@type MIMETYPE_REDIRECT: L{str}
@var CHECKSUM_LENGTH: the length of the checksum in a ZIM file, in bytes
@type CHECKSUM_LENGTH: L{int}
@var DEFAULT_COMPRESSION: default compression type to use
@type DEFAULT_COMPRESSION: L{pyzim.compression.CompressionType}
@var LOG_LEVEL_ALLOCATION: log level used for allocation logging
@type LOG_LEVEL_ALLOCATION: L{int}
@var LOG_LEVEL_WRITE: log level for individual write operations
@type LOG_LEVEL_WRITE: L{int}
@var LOG_LEVEL_READ: log level for individual read operations
@type LOG_LEVEL_READ: L{int}
"""
from .compression import CompressionType


# major version of the ZIM standard implemented
ZIM_MAJOR_VERSION = 6
# minor version of the ZIM standard implemented
# TODO: we probably need multiple values here later
ZIM_MINOR_VERSION = 1
# list of compatible ZIM versions
COMPATIBLE_ZIM_VERSIONS = (ZIM_MAJOR_VERSION, )

# encoding used
ENCODING = "utf-8"

# integer endian-ness, in struct format
ENDIAN = "<"

# special URLs
URL_ENTRY_TITLE_INDEX = "Xlisting/titleOrdered/v0"
URL_ARTICLE_TITLE_INDEX = "Xlisting/titleOrdered/v1"
URL_MAINPAGE_REDIRECT = "WmainPage"
URL_COUNTER = "MCounter"

# mimetypes
MIMETYPE_ZIMLISTING = "application/octet-stream+zimlisting"
# special symbolic mimetype for redirects
MIMETYPE_REDIRECT = "<redirect>"

# length of checksum
CHECKSUM_LENGTH = 16

# compression constants
DEFAULT_COMPRESSION = CompressionType.ZSTD

# special log levels
LOG_LEVEL_ALLOCATION = 5
LOG_LEVEL_WRITE = 6
LOG_LEVEL_READ = 4
LOG_LEVEL_COMPRESSION_STRATEGY = 7
