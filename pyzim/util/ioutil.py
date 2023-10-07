"""
Various I/O related utility functions.
"""


def read_until_zero(f, encoding=None, strip_zero=True):
    """
    Read a zero-terminated bytestring from a file.

    @param f: file-like object to read from
    @type f: file-like object
    @param encoding: if specified, decode the string using this encoding
    @type encoding: L{str} or L{None}
    @param strip_zero: if nonzero (default), do not add the zero to the result string
    @type strip_zero: L{bool}
    @return: the parsed string.
    @rtype: L{bytes} or L{str} if an encoding was specified
    """
    s = b""
    while True:
        c = f.read(1)
        if c == b"\x00":
            if not strip_zero:
                s += c
            break
        else:
            s += c
    if encoding is not None:
        return s.decode(encoding)
    else:
        return s


def read_n_bytes(f, n, raise_on_incomplete=False):
    """
    Read n bytes in total from f.

    If raise_on_incomplete is zero, this may return less bytes on EOF.
    Otherwise, an exception is raised.

    @param f: file-like object to read from
    @type f: file-like
    @param n: number of bytes to read
    @type n: L{int}
    @param raise_on_incomplete: if nonzero, raise an Exception if unable to read full n bytes
    @type raise_on_incomplete: L{bool}
    @return: the content read
    @rtype: L{bytes}
    @raises IOError: when raise_in_incomplete is nonzero and unable to read full n bytes.
    """
    rbuff = b""
    remaining_read = n
    while remaining_read > 0:
        data = f.read(remaining_read)
        if not data:
            if remaining_read and raise_on_incomplete:
                raise IOError("Encountered EOF before reading full {} bytes ({} read)!".format(n, len(rbuff)))
            break
        rbuff += data
        remaining_read -= len(data)
    return rbuff
