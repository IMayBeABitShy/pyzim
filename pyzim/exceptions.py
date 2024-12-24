"""
Exception definitions.
"""


class BaseZimException(Exception):
    """
    Base class for all ZIM-related exceptions.

    This allows the user to simply catch all such exceptions at once.
    """
    pass


class ZimFileClosed(BaseZimException):
    """
    Exception raised when trying to operate on a closed ZIM file.
    """
    pass


class IncompatibleZimFile(BaseZimException):
    """
    Exception raised when a ZIM file is not compatible.
    """
    pass


class NotAZimFile(BaseZimException):
    """
    Exception raised when a file does not appear to be a ZIM file.
    """
    pass


class EntryNotFound(BaseZimException):
    """
    Exception raised when an entry was not found.
    """
    pass


class ParseError(BaseZimException):
    """
    Exception raised when a parsing failed!
    """
    pass


class BindingError(BaseZimException):
    """
    Baseclass for exceptions caused by binding-related functionality.
    """
    pass


class AlreadyBound(BindingError):
    """
    Exception raised when attempting to bind an already bound object.
    """
    pass


class BindRequired(BindingError):
    """
    Exception raised when an operation requiring a bound object is performed on an unbound object.
    """
    pass


class UnsupportedCompressionType(IncompatibleZimFile):
    """
    Exception raised when no compressor/decompressor is known for a specific compression type.
    """
    pass


class UnsortedList(BaseZimException):
    """
    Exception raised when a list is not sorted even though it is supposed to be.
    """
    pass


class BlobNotFound(BaseZimException):
    """
    Exception raised when a blob was not found inside a cluster.
    """
    pass


class NoCounter(BaseZimException):
    """
    Exception raised when the counter could not be loaded.
    """
    pass


class OperationNotSupported(BaseZimException):
    """
    Exception raised when a class does not support an operation.
    """
    pass


class ZimWriteException(BaseZimException):
    """
    Baseclass for exception related to ZIM creation/modification.
    """
    pass


class NonMutable(ZimWriteException, OperationNotSupported):
    """
    Exception raised when a non-mutable/non-modifiable object is modified.
    """
    pass


class UnresolvedRedirect(ZimWriteException):
    """
    Exception raised when a redirect can not be resolved during writing.
    """
    pass
