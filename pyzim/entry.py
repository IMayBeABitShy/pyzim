"""
Implementation of entries.
"""
import struct

from . import constants
from .exceptions import BindRequired
from .bindable import BindableMixIn
from .ioutil import read_until_zero


class BaseEntry(BindableMixIn):
    """
    Baseclass for entries in a ZIM file.

    An entry stores informations about an article/file in the ZIM file.

    @cvar MIMETYPE_ID_REDIRECT: mimetype index that identifies redirects
    @type MIMETYPE_ID_REDIRECT: L{int}
    @cvar FORMAT_MT: the struct format to parse the mimetype
    @type FORMAT_MT: L{str}
    @cvar LENGTH_MT: length of the mimetype part of the struct
    @type LENGTH_MT: L{int}

    @ivar mimetype_id: index of mimetype in the mimetypelist.
    @type mimetype_id: L{int}
    @ivar namespace: namespace this entry is part of
    @type namespace: L{str}
    @ivar revision: the revision of the content of this entry. Unused, must be 0.
    @type revision: L{int}
    @ivar url: url of this entry
    @type url: L{str}
    @ivar title: title of this entry
    @type title: L{str}
    @ivar parameters: extra parameters. Unused, must be None.
    @type parameters: L{None}
    """
    FORMAT_MT = constants.ENDIAN + "H"
    LENGTH_MT = struct.calcsize(FORMAT_MT)

    # special mimetypes
    MIMETYPE_ID_REDIRECT = 0xffff

    def __init__(
        self,
        mimetype_id,
        namespace,
        revision,
        url,
        title,
        parameters,
    ):
        """
        The default constructor.

        @param mimetype_id: index of mimetype in the mimetypelist.
        @type mimetype_id: L{int}
        @param namespace: namespace this entry is part of
        @type namespace: L{str}
        @param revision: the revision of the content of this entry. Unused, must be 0.
        @type revision: L{int}
        @param url: url of this entry
        @type url: L{str}
        @param title: title of this entry
        @type title: L{str}
        @param parameters: extra parameters. Unused, must be empty.
        @type parameters: L{list}
        """
        assert isinstance(mimetype_id, int) and mimetype_id >= 0
        assert isinstance(namespace, str)
        assert isinstance(revision, int) and revision == 0
        assert isinstance(url, str)
        assert isinstance(title, str)
        assert isinstance(parameters, list) and len(parameters) == 0

        BindableMixIn.__init__(self)

        self.mimetype_id = mimetype_id
        self.namespace = namespace
        self.revision = revision
        self.url = url
        self.title = title
        self.parameters = parameters

    @property
    def is_redirect(self):
        """
        True if this entry is a redirect, False otherwise.

        @return: True if this entry is a redirect, False otherwise
        @rtype: L{bool}
        """
        return self.mimetype_id == self.MIMETYPE_ID_REDIRECT

    @property
    def full_url(self):
        """
        The full URL, composed of namespace and url.

        @return: the full URL
        @rtype: L{str}
        """
        return self.namespace + self.url

    @classmethod
    def from_file(cls, f, seek=None):
        """
        Read an entry from a file.

        @param f: file-like object to read from
        @type f: file-like
        @param seek: if specified, seek this position of the file before reading
        @type seek: L{int} or L{None}
        @return: the entry in the correct subclass
        @rtype: L{pyzim.entry.BaseEntry} (or subclass thereof)
        """
        assert isinstance(seek, int) or (seek is None)
        if seek is not None:
            f.seek(seek)
        mt_data = f.read(cls.LENGTH_MT)
        mimetype = struct.unpack(cls.FORMAT_MT, mt_data)[0]

        if mimetype == cls.MIMETYPE_ID_REDIRECT:
            return RedirectEntry.from_file(f, mimetype=mimetype)
        else:
            return ContentEntry.from_file(f, mimetype=mimetype)

    @property
    def mimetype(self):
        """
        The mimetype as string. Only available when bound.

        @return: if bound, the mimetype as string
        @rtype: L{str}
        @raise pyzim.exceptions.BindRequired: when not bound
        """
        if not self.bound:
            raise BindRequired("Accessing mimetype directly requires the entry to be bound!")
        return self.zim.get_mimetype_of_entry(self)

    def resolve(self):
        """
        If this entry is a redirect, follow this redirect and any subsequent
        redirects until a non-redirect is reached and return it. Otherwise,
        return this entry.

        This requires this entry to be bound.

        @return: the first non-redirect entry
        @rtype: L{pyzim.entry.BaseEntry}
        @raises pyzim.exceptions.BindRequired: if not bound
        """
        return self  # default case, overwritten in subclasses


class ContentEntry(BaseEntry):
    """
    An entry for actual content (e.g. an article).

    @cvar FORMAT_NMT: the format of this entry, excluding the mimetype
    @type FORMAT_NMT: L{str}
    @cvar LENGTH_FMT: length of this entry, excluding mimetype and zero terminated parts
    @type LENGTH_FMT: L{int}

    @ivar cluster_number: number of the cluster the content of this entry is stored in
    @type cluster_number: L{int}
    @ivar blob_number: number of the blob within the specified cluster where the content of this entry is stored
    @type blob_number: L{int}
    """
    FORMAT_NMT = constants.ENDIAN + "BcIII"
    LENGTH_NMT = struct.calcsize(FORMAT_NMT)

    def __init__(
        self,
        mimetype,
        namespace,
        revision,
        cluster_number,
        blob_number,
        url,
        title,
        parameters,
    ):
        """
        The default constructor.

        @param mimetype: index of mimetype in the mimetypelist.
        @type mimetype: L{int}
        @param namespace: namespace this entry is part of
        @type namespace: L{str}
        @param revision: the revision of the content of this entry. Unused, must be 0.
        @type revision: L{int}
        @param cluster_number: number of the cluster the content of this entry is stored in
        @type cluster_number: L{int}
        @param blob_number: number of the blob within the specified cluster where the content of this entry is stored
        @type blob_number: L{int}
        @param url: url of this entry
        @type url: L{str}
        @param title: title of this entry
        @type title: L{str}
        @param parameters: extra parameters. Unused, must be empty.
        @type parameters: L{list}
        """
        BaseEntry.__init__(self, mimetype, namespace, revision, url, title, parameters)
        self.cluster_number = cluster_number
        self.blob_number = blob_number

    @classmethod
    def from_file(cls, f, mimetype=None, seek=None):
        """
        Read a content entry from a file.

        @param f: file-like object to read from
        @type f: file-like
        @param mimetype: index of mimetype. If specified, do not read it from file
        @type mimetype: L{int}
        @param seek: if specified, seek this position of the file before reading
        @type seek: L{int} or L{None}
        @return: the entry read from the file
        @rtype: L{pyzim.entry.ContentEntry}
        """
        assert isinstance(mimetype, int) or (mimetype is None)
        assert isinstance(seek, int) or (seek is None)
        if seek is not None:
            f.seek(seek)
        if mimetype is None:
            mt_data = f.read(cls.LENGTH_MT)
            mimetype = struct.unpack(cls.FORMAT_MT, mt_data)[0]
        data = f.read(cls.LENGTH_NMT)
        parsed = struct.unpack(cls.FORMAT_NMT, data)
        n_parameters, namespace, revision, cluster_number, blob_number = parsed
        namespace = namespace.decode(constants.ENCODING)
        url = read_until_zero(f).decode(constants.ENCODING)
        title = read_until_zero(f).decode(constants.ENCODING)
        if not title:
            title = url
        parameters = [read_until_zero(f).decode(constants.ENCODING) for i in range(n_parameters)]
        return cls(
            mimetype,
            namespace,
            revision,
            cluster_number,
            blob_number,
            url,
            title,
            parameters,
        )

    def get_cluster(self):
        """
        Get the cluster containing the blob containing the content of this entry.

        @return: the cluster, which contains the blob that contains the content of this entry
        @rtype: L{pyzim.cluster.Cluster}
        """
        if not self.bound:
            raise BindRequired("Accessing the cluster of this entry requires this entry to be bound!")
        return self.zim.get_cluster_by_index(self.cluster_number)

    def get_size(self):
        """
        Return the (decompressed) size of the content of this entry.

        @return: the size of the content of this entry
        @rtype: L{int}
        """
        if not self.bound:
            raise BindRequired("Accessing the size of this entry requires this entry to be bound!")
        cluster = self.get_cluster()
        return cluster.get_blob_size(self.blob_number)

    def read(self):
        """
        Read the content of this entry.

        @return: the content of this entry
        @rtype: L{bytes}
        """
        if not self.bound:
            raise BindRequired("Reading the content of this entry requires this entry to be bound!")
        cluster = self.get_cluster()
        return cluster.read_blob(self.blob_number)

    def iter_read(self, buffersize=4096):
        """
        Read the content of this entry iteratively.

        @param buffersize: number of bytes to read at once
        @type buffersize: L{int}
        @yields: chunks of the content of this entry in sequential order
        @ytype: L{bytes}
        """
        if not self.bound:
            raise BindRequired("Reading the content of this entry requires this entry to be bound!")
        cluster = self.get_cluster()
        yield from cluster.iter_read_blob(self.blob_number, buffersize=buffersize)


class RedirectEntry(BaseEntry):
    """
    An entry for a redirect.

    @cvar FORMAT_NMT: the format of this entry, excluding the mimetype
    @type FORMAT_NMT: L{str}
    @cvar LENGTH_FMT: length of this entry, excluding mimetype and zero terminated parts
    @type LENGTH_FMT: L{int}

    @ivar redirect_index: position/offset of the entry this entry redirects to
    @type redirect_index: L{int}
    """
    FORMAT_NMT = constants.ENDIAN + "BcII"
    LENGTH_NMT = struct.calcsize(FORMAT_NMT)

    def __init__(
        self,
        mimetype,
        namespace,
        revision,
        redirect_index,
        url,
        title,
        parameters,
    ):
        """
        The default constructor.

        @param mimetype: index of mimetype in the mimetypelist.
        @type mimetype: L{int}
        @param namespace: namespace this entry is part of
        @type namespace: L{str}
        @param revision: the revision of the content of this entry. Unused, must be 0.
        @type revision: L{int}
        @param redirect_index: position/offset of the entry this entry redirects to
        @type redirect_index: L{int}
        @param url: url of this entry
        @type url: L{str}
        @param title: title of this entry
        @type title: L{str}
        @param parameters: extra parameters. Unused, must be empty.
        @type parameters: L{list}
        """
        BaseEntry.__init__(self, mimetype, namespace, revision, url, title, parameters)
        self.redirect_index = redirect_index

    @classmethod
    def from_file(cls, f, mimetype=None, seek=None):
        """
        Read a content entry from a file.

        @param f: file-like object to read from
        @type f: file-like
        @param mimetype: index of mimetype. If specified, do not read it from file
        @type mimetype: L{int}
        @param seek: if specified, seek this position of the file before reading
        @type seek: L{int} or L{None}
        @return: the entry read from the file
        @rtype: L{pyzim.entry.RedirectEntry}
        """
        assert isinstance(mimetype, int) or (mimetype is None)
        assert isinstance(seek, int) or (seek is None)
        if seek is not None:
            f.seek(seek)
        if mimetype is None:
            mt_data = f.read(cls.LENGTH_MT)
            mimetype = struct.unpack(cls.FORMAT_MT, mt_data)[0]
        data = f.read(cls.LENGTH_NMT)
        parsed = struct.unpack(cls.FORMAT_NMT, data)
        n_parameters, namespace, revision, redirect_index = parsed
        namespace = namespace.decode(constants.ENCODING)
        url = read_until_zero(f).decode(constants.ENCODING)
        title = read_until_zero(f).decode(constants.ENCODING)
        if not title:
            title = url
        parameters = [read_until_zero(f).decode(constants.ENCODING) for i in range(n_parameters)]
        return cls(
            mimetype,
            namespace,
            revision,
            redirect_index,
            url,
            title,
            parameters,
        )

    def follow(self):
        """
        Follow this redirect and the next entry.

        This requires this entry to be bound. The next entry may be a
        redirect too.

        @return: the next entry
        @rtype: L{pyzim.entry.BaseEntry}
        @raises pyzim.exceptions.BindRequired: if not bound
        """
        if not self.bound:
            raise BindRequired("Following redirects requires this entry to be bound!")
        return self.zim.get_entry_by_url_index(self.redirect_index)

    def resolve(self):
        """
        Follow this redirect and any subsequent redirects until a non-redirect is reached.

        This requires this entry to be bound.

        @return: the first non-redirect entry
        @rtype: L{pyzim.entry.BaseEntry}
        @raises pyzim.exceptions.BindRequired: if not bound
        """
        entry = self
        while entry.is_redirect:
            entry = entry.follow()
        return entry
