"""
Implementation of entries.
"""
import struct

from . import constants
from .exceptions import BindRequired, ParseError
from .bindable import BindableMixIn
from .modifiable import ModifiableMixIn
from .blob import BaseBlobSource, InMemoryBlobSource
from .util.ioutil import read_until_zero


class BaseEntry(BindableMixIn, ModifiableMixIn):
    """
    Baseclass for entries in a ZIM file.

    An entry stores informations about an article/file in the ZIM file.

    @cvar MIMETYPE_ID_REDIRECT: mimetype index that identifies redirects
    @type MIMETYPE_ID_REDIRECT: L{int}
    @cvar FORMAT_MT: the struct format to parse the mimetype
    @type FORMAT_MT: L{str}
    @cvar LENGTH_MT: length of the mimetype part of the struct
    @type LENGTH_MT: L{int}

    @ivar _mimetype_id: index of mimetype in the mimetypelist.
    @type _mimetype_id: L{int}
    @ivar _namespace: namespace this entry is part of
    @type _namespace: L{str}
    @ivar _revision: the revision of the content of this entry. Unused, must be 0.
    @type _revision: L{int}
    @ivar _url: url of this entry
    @type _url: L{str}
    @ivar _title: title of this entry
    @type _title: L{str}
    @ivar _parameters: extra parameters. Unused, must be None.
    @type _parameters: L{None}
    @ivar _old_full_url: full URL of entry as of last read/flush
    @type _old_full_url: L{str}
    @ivar _force_is_article: force-set the article state of this entry
    @type _force_is_article: L{bool} or L{None}
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
        @type title: L{str} or L{None}
        @param parameters: extra parameters. Unused, must be empty.
        @type parameters: L{list}
        """
        assert isinstance(mimetype_id, int) and mimetype_id >= 0
        assert isinstance(namespace, str)
        assert isinstance(revision, int) and revision == 0
        assert isinstance(url, str)
        assert isinstance(title, str) or title is None
        assert isinstance(parameters, list) and len(parameters) == 0

        BindableMixIn.__init__(self)
        ModifiableMixIn.__init__(self)

        self._mimetype_id = mimetype_id
        self._namespace = namespace
        self._revision = revision
        self._url = url
        self._title = title
        self._parameters = parameters

        self._old_full_url = None
        self._force_is_article = None

        # we do not call ModifiableMixIn.after_flush_or_read as we
        # only know the size in subclasses

    # ========== properties ===========

    @property
    def mimetype_id(self):
        """
        The index of the mimetype in the mimetypelist.

        @return: the index of the mimetype in the mimetypelist
        @rtype: L{int}
        """
        return self._mimetype_id

    @mimetype_id.setter
    def mimetype_id(self, value):
        """
        The index of the mimetype in the mimetypelist.

        @param value: value to set
        @type value: L{int}
        @raises TypeError: if value is not an integer
        @raises pyzim.exceptions.NonMutable: if this entry is set to be inmutable.
        @raises ValueError: if the value is invalid (e.g. negative)
        """
        if not isinstance(value, int):
            raise TypeError("Expected an integer, got {}!".format(type(value)))
        if value < 0:
            raise ValueError("Value can not be negative!")
        self.ensure_mutable()
        if value != self._mimetype_id:
            self._mimetype_id = value
            self.mark_dirty()

    @property
    def namespace(self):
        """
        The namespace this entry is part of.

        @return: the namespace this entry is part of.
        @rtype: L{str}
        """
        return self._namespace

    @namespace.setter
    def namespace(self, value):
        """
        The namespace this entry is part of.

        If the namespace is not C{"C"}, L{BaseEntry.is_article} will be
        set to C{False}.

        @param value: value to set
        @type value: L{str} or L{bytes}
        @raises TypeError: if value is not a string or bytes.
        @raises pyzim.exceptions.NonMutable: if this entry is set to be inmutable.
        @raises ValueError: if the value is invalid (e.g. length != 1)
        """
        if not isinstance(value, (str, bytes)):
            raise TypeError("Expected a string or bytes, got {}!".format(type(value)))
        if isinstance(value, str):
            bstring = value.encode(constants.ENCODING)
            ustring = value
        else:
            bstring = value
            ustring = value.decode(constants.ENCODING)
        if len(bstring) != 1:
            raise ValueError("Value must be length 1 (encoded)!")
        self.ensure_mutable()
        if ustring != self._namespace:
            self._namespace = ustring
            if ustring != "C":
                self._force_is_article = False
            self.mark_dirty()

    @property
    def revision(self):
        """
        The revision of the content of this entry. Unused, must be 0.

        @return: the revision of the content of this entry.
        @rtype: L{int}
        """
        return self._revision

    @revision.setter
    def revision(self, value):
        """
        The revision of the content of this entry. Unused, must be 0..

        @param value: value to set
        @type value: L{int}
        @raises TypeError: if value is not an integer
        @raises pyzim.exceptions.NonMutable: if this entry is set to be inmutable.
        @raises ValueError: if the value is invalid (currently, != 0 due to not being implemented by the standard)
        """
        if not isinstance(value, int):
            raise TypeError("Expected an integer, got {}!".format(type(value)))
        if value != 0:
            raise ValueError("Value must be 0! Currently, the ZIM standard does not allows other values.")
        self.ensure_mutable()
        if value != self._revision:
            self._revision = value
            self.mark_dirty()

    @property
    def url(self):
        """
        The URL of this entry.

        @return: the URL of this entry.
        @rtype: L{str}
        """
        return self._url

    @url.setter
    def url(self, value):
        """
        The URL of this entry.

        @param value: value to set
        @type value: L{str} or L{bytes}
        @raises TypeError: if value is not a string nor bytes.
        @raises pyzim.exceptions.NonMutable: if this entry is set to be inmutable.
        @raises ValueError: if the value is invalid (e.g. contains null bytes)
        """
        if not isinstance(value, (str, bytes)):
            raise TypeError("Expected a string or bytes, got {}!".format(type(value)))
        if isinstance(value, str):
            bstring = value.encode(constants.ENCODING)
            ustring = value
        else:
            bstring = value
            ustring = value.decode(constants.ENCODING)
        if b"\x00" in bstring:
            raise ValueError("Value can not contain null bytes!")
        self.ensure_mutable()
        if ustring != self._url:
            self._url = ustring
            self.mark_dirty()

    @property
    def title(self):
        """
        The title of this entry.

        If the title is empty, the URL will be used instead.

        @return: the title of this entry.
        @rtype: L{str}
        """
        if not self._title:
            return self.url
        return self._title

    @title.setter
    def title(self, value):
        """
        The title of this entry.

        @param value: value to set
        @type value: L{str} or L{bytes} or L{None}
        @raises TypeError: if value is not a string nor bytes nor L{None}.
        @raises pyzim.exceptions.NonMutable: if this entry is set to be inmutable.
        @raises ValueError: if the value is invalid (e.g. contains null bytes)
        """
        if (not isinstance(value, (str, bytes))) and (value is not None):
            raise TypeError("Expected a string or bytes or None, got {}!".format(type(value)))
        if value is None:
            ustring = u""
            bstring = b""
        elif isinstance(value, str):
            bstring = value.encode(constants.ENCODING)
            ustring = value
        else:
            bstring = value
            ustring = value.decode(constants.ENCODING)
        if b"\x00" in bstring:
            raise ValueError("Value can not contain null bytes!")
        self.ensure_mutable()
        if ustring != self._title:
            self._title = ustring
            self.mark_dirty()

    @property
    def parameters(self):
        """
        The extra parameters of this entry.

        Currently, this must be 0.

        @return: the extra parameters of this entry.
        @rtype: L{list}
        """
        return self._parameters

    @parameters.setter
    def parameters(self, value):
        """
        The extra parameters of this entry.

        @param value: value to set
        @type value: L{list}
        @raises TypeError: if value is not the expected type.
        @raises pyzim.exceptions.NonMutable: if this entry is set to be inmutable.
        @raises ValueError: if the value is invalid.
        """
        if (not isinstance(value, list)):
            raise TypeError("Expected a list, got {}!".format(type(value)))
        if value:
            raise ValueError("Currently, the ZIM standard requires parameters to be empty!")
        self.ensure_mutable()
        if value != self._parameters:
            self._parameters = value
            self.mark_dirty()

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

    @full_url.setter
    def full_url(self, value):
        """
        The full URL, composed of namespace and url.

        @param value: value to set
        @type value: L{str} or L{bytes}
        @raises TypeError: on invalid type
        @raises ValueError: on invalid value (e.g. empty)
        @raises L{pyzim.exceptions.NonMutable}: if this entry is set to be immutable.
        """
        if not isinstance(value, (str, bytes)):
            raise TypeError("Expected str or bytes, got {}!".format(type(value)))
        if isinstance(value, str):
            ustring = value
        else:
            ustring = value.decode(constants.ENCODING)
        if len(ustring) < 1:
            raise ValueError("Full URL needs to have a length of at least 1 (due to namespace)!")
        # mutable and dirty behavior will be handled by recursive assignment
        namespace, url = ustring[0], ustring[1:]
        self.namespace = namespace
        self.url = url

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

    @mimetype.setter
    def mimetype(self, value):
        """
        The mimetype as a string. Only available when bound.

        @param value: value to set
        @type value: L{str} or L{bytes}
        @raises TypeError: on invalid type
        @raises ValueError: if value is invalid (e.g. empty)
        @raises pyzim.exceptions.NonMutable: if this entry is set to be immutable
        @raises pyzim.exceptions.BindRequired: when not bound
        """
        if not isinstance(value, (str, bytes)):
            raise TypeError("Expected str or bytes, got {} instead!".format(type(value)))
        if isinstance(value, str):
            ustring = value
            bstring = value.encode(constants.ENCODING)
        else:
            ustring = value.decode(constants.ENCODING)
            bstring = value
        if len(bstring) == 0:
            raise ValueError("Mimetype can not be empty!")
        if b"\x00" in bstring:
            raise ValueError("Mimetype can not contain null bytes!")
        if not self.bound:
            raise BindRequired("Setting the mimetype directly requires the entry to be bound!")
        # dirty and mutable behavior will be handled by recursive assignment
        self.mimetype_id = self.zim.mimetypelist.get_index(ustring, register=True)

    @property
    def is_article(self):
        """
        Whether this entry is an article or not.

        @return: whether this entry is an article or not
        @rtype: L{bool}
        @raises pyzim.exceptions.BindRequired: when not bound
        """
        if self._force_is_article is not None:
            return self._force_is_article
        if not self.bound:
            raise BindRequired("Getting the non-force-set article status requires the entry to be bound!")
        if self.namespace != "C":
            # can not be an article if in "C" namespace
            return False
        return self.zim.entry_at_url_is_article(self.unmodified_full_url)

    @is_article.setter
    def is_article(self, value):
        """
        Whether this entry is an article or not.

        @param value: value to set
        @type value: L{bool}
        @raises TypeError: on invalid type
        @raises ValueError: if value is incompatible with namespace
        @raises pyzim.exceptions.NonMutable: if this entry is set to be immutable
        """
        if not isinstance(value, bool):
            raise TypeError("Expected a bool, got {} instead!".format(type(value)))
        self.ensure_mutable()
        if value and self.namespace != "C" and self.namespace is not None:
            raise ValueError("Articles can not exists outside of C namespace!")
        if value != self._force_is_article:
            self._force_is_article = value
            self.mark_dirty()

    # =========== serialization ============

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

    def get_disk_size(self):
        raise NotImplementedError("Subclasses of BaseEntry must implement get_disk_size()!")

    def _get_mimetype_size(self):
        """
        Return the size of the mimetype on disk in bytes.

        This is a helper method that may be called by subclasses to
        help their implementation of L{BaseEntry.get_disk_size}-

        @return: the size of the mimetype on disk, in bytes
        @rtype: L{int}
        """
        return self.LENGTH_MT

    def to_bytes(self):
        """
        Serialize this entry into a bytestring and return it.

        @return: a bytestring representing the content of this entry.
        @rtype: L{bytes}
        """
        raise NotImplementedError("Subclasses of BaseEntry must implement to_bytes()!")

    def flush(self):
        """
        Write this entry to the archive if it is dirty.

        @raises pyzim.exceptions.NonMutable: if this entry is set to be immutable
        @raises pyzim.exceptions.BindRequired: when not bound
        """
        if not self.bound:
            raise BindRequired("This entry must be bound to an archive to flush() it!")
        self.ensure_mutable()
        if self.dirty:
            self.zim.write_entry(self)

    def after_flush_or_read(self):
        # entries are identified by their full URL
        # thus, if we want to figure out the position of an entry in a
        # pointer list, we need to keep track of the full URL as it is
        # on disk
        self._old_full_url = self.namespace + self.url
        ModifiableMixIn.after_flush_or_read(self)

    @property
    def unmodified_full_url(self):
        """
        The full URL of this entry of the time this entry was last read or flushed.

        @return: the old full URL of this entry
        @rtype: L{str}
        """
        return self._old_full_url

    # ========== default methods ==========

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

    def remove(self, blob="empty"):
        """
        Remove this entry from the archive it is bound to.

        @param blob: passed to L{pyzim.archive.Zim.remove_entry_by_full_url}
        @type blob: L{str}
        @raises TypeError: on invalid type
        @raises ValueError: on invalid value
        @raises pyzim.exceptions.BindRequired: when not bound
        @raises pyzim.exceptions.NonMutable: if this entry is set to be inmutable.
        """
        if not self.bound:
            raise BindRequired("Removing an entry from an archive requires the entry to be bound!")
        self.ensure_mutable()
        self.zim.remove_entry_by_full_url(self.full_url, blob=blob)


class ContentEntry(BaseEntry):
    """
    An entry for actual content (e.g. an article).

    @cvar FORMAT_NMT: the format of this entry, excluding the mimetype
    @type FORMAT_NMT: L{str}
    @cvar LENGTH_FMT: length of this entry, excluding mimetype and zero terminated parts
    @type LENGTH_FMT: L{int}

    @ivar _cluster_number: number of the cluster the content of this entry is stored in
    @type _cluster_number: L{int}
    @ivar _blob_number: number of the blob within the specified cluster where the content of this entry is stored
    @type _blob_number: L{int}
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
        self._cluster_number = cluster_number
        self._blob_number = blob_number

        # ensure we know the current object size before modifications later
        self.after_flush_or_read()

    # ========== properties ==========

    @property
    def cluster_number(self):
        """
        The number of the cluster the content of this entry is stored in.

        @return: the number of the cluster the content of this entry is stored in.
        @rtype: L{int}
        """
        return self._cluster_number

    @cluster_number.setter
    def cluster_number(self, value):
        """
        The number of the cluster the content of this entry is stored in

        @param value: value to set
        @type value: L{int}
        @raises TypeError: if value is not an integer
        @raises pyzim.exceptions.NonMutable: if this entry is set to be inmutable.
        @raises ValueError: if the value is invalid (e.g. negative)
        """
        if not isinstance(value, int):
            raise TypeError("Expected an integer, got {}!".format(type(value)))
        if value < 0:
            raise ValueError("Value can not be negative!")
        self.ensure_mutable()
        if value != self._cluster_number:
            self._cluster_number = value
            self.mark_dirty()

    @property
    def blob_number(self):
        """
        The number of the blob within the specified cluster where the content of this entry is stored.

        @return: the number of the blob within the specified cluster where the content of this entry is stored.
        @rtype: L{int}
        """
        return self._blob_number

    @blob_number.setter
    def blob_number(self, value):
        """
        The number of the blob within the specified cluster where the content of this entry is stored.

        @param value: value to set
        @type value: L{int}
        @raises TypeError: if value is not an integer
        @raises pyzim.exceptions.NonMutable: if this entry is set to be inmutable.
        @raises ValueError: if the value is invalid (e.g. negative)
        """
        if not isinstance(value, int):
            raise TypeError("Expected an integer, got {}!".format(type(value)))
        if value < 0:
            raise ValueError("Value can not be negative!")
        self.ensure_mutable()
        if value != self._blob_number:
            self._blob_number = value
            self.mark_dirty()

    # =============== serialization ==============

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

    def get_disk_size(self):
        url_size = len(self.url.encode(constants.ENCODING)) + 1  # 1 for the null byte
        if self._title:
            title_size = len(self.title.encode(constants.ENCODING)) + 1  # 1 for the null byte
        else:
            title_size = 1  # for the 0 byte
        if self.parameters:
            # parameter handling not yet implemented
            raise NotImplementedError("get_disk_size() does not yet support parameters!")
        parameters_size = 0  # needs to be changed later
        size = BaseEntry._get_mimetype_size(self) + self.LENGTH_NMT + url_size + title_size + parameters_size
        return size

    def to_bytes(self):
        data = struct.pack(
            self.FORMAT_MT + self.FORMAT_NMT[1:],  # [1:] to get rid of endian
            self.mimetype_id,
            0,  # parameter len,
            self.namespace.encode(constants.ENCODING),
            0,  # revision,
            self.cluster_number,
            self.blob_number,
        )
        data += self.url.encode(constants.ENCODING) + b"\x00"
        data += self._title.encode(constants.ENCODING) + b"\x00"
        # if any parameters were here, they would now be appended
        return data

    # =========== content methods ==============

    def get_cluster(self):
        """
        Get the cluster containing the blob containing the content of this entry.

        @return: the cluster, which contains the blob that contains the content of this entry
        @rtype: L{pyzim.cluster.Cluster}
        @raises pyzim.exceptions.BindRequired: when not bound
        """
        if not self.bound:
            raise BindRequired("Accessing the cluster of this entry requires this entry to be bound!")
        return self.zim.get_cluster_by_index(self.cluster_number)

    def get_size(self):
        """
        Return the (decompressed) size of the content of this entry.

        @return: the size of the content of this entry
        @rtype: L{int}
        @raises pyzim.exceptions.BindRequired: when not bound
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
        @raises pyzim.exceptions.BindRequired: when not bound
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
        @raises pyzim.exceptions.BindRequired: when not bound
        """
        if not self.bound:
            raise BindRequired("Reading the content of this entry requires this entry to be bound!")
        cluster = self.get_cluster()
        yield from cluster.iter_read_blob(self.blob_number, buffersize=buffersize)

    def set_content(self, blob_source):
        """
        Set the content of this entry.

        This method does not flush the modified cluster, but it does
        return the cluster, so you can just call .flush() on this.

        @param blob_source: blob source or str/bytes to use as content
        @type blob_source: L{pyzim.blob.BaseBlobSource} or L{str} or L{bytes}
        @return: the cluster this blob is part of
        @rtype: L{pyzim.cluster.ModifiableClusterWrapper}
        @raises TypeError: on invalid type
        @raises pyzim.exceptions.BindRequired: when not bound
        @raises pyzim.exceptions.NonMutable: if this entry is set to be inmutable.
        """
        if not isinstance(blob_source, (BaseBlobSource, str, bytes)):
            raise TypeError("Expected a BaseBlobSource, string or bytes, got {} instead!".format(type(blob_source)))
        if not self.bound:
            raise BindRequired("Setting the content of an entry from an archive requires the entry to be bound!")
        self.ensure_mutable()

        if isinstance(blob_source, (str, bytes)):
            blob_source = InMemoryBlobSource(blob_source)
        cluster = self.get_cluster()
        cluster.set_blob(self.blob_number, blob_source)
        return cluster


class RedirectEntry(BaseEntry):
    """
    An entry for a redirect.

    @cvar FORMAT_NMT: the format of this entry, excluding the mimetype
    @type FORMAT_NMT: L{str}
    @cvar LENGTH_FMT: length of this entry, excluding mimetype and zero terminated parts
    @type LENGTH_FMT: L{int}

    @ivar _redirect_index: position/offset of the entry in the URL entry list this entry redirects to
    @type _redirect_index: L{int}
    """
    FORMAT_NMT = constants.ENDIAN + "BcII"
    LENGTH_NMT = struct.calcsize(FORMAT_NMT)

    def __init__(
        self,
        namespace,
        revision,
        redirect_index,
        url,
        title,
        parameters,
    ):
        """
        The default constructor.

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
        BaseEntry.__init__(self, self.MIMETYPE_ID_REDIRECT, namespace, revision, url, title, parameters)
        self._redirect_index = redirect_index

        # ensure we know the current object size before modifications later
        self.after_flush_or_read()

    # ================ properties =============

    @property
    def redirect_index(self):
        """
        The position/offset of the entry in the URL entry list this entry redirects to.

        @return: the position/offset of the entry in the URL entry list this entry redirects to-
        @rtype: L{int}
        """
        return self._redirect_index

    @redirect_index.setter
    def redirect_index(self, value):
        """
        The position/offset of the entry in the URL entry list this entry redirects to.

        @param value: value to set
        @type value: L{int}
        @raises TypeError: if value is not an integer
        @raises pyzim.exceptions.NonMutable: if this entry is set to be inmutable.
        @raises ValueError: if the value is invalid (e.g. negative)
        """
        if not isinstance(value, int):
            raise TypeError("Expected an integer, got {}!".format(type(value)))
        if value < 0:
            raise ValueError("Value can not be negative!")
        self.ensure_mutable()
        if value != self._redirect_index:
            self._redirect_index = value
            self.mark_dirty()

    # ============ serialization =============

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
        if mimetype != cls.MIMETYPE_ID_REDIRECT:
            # this is not a redirect
            raise ParseError("Error parsing redirect: redirect mimetype should be {}, but got {} instead!".format(cls.MIMETYPE_ID_REDIRECT, mimetype))
        data = f.read(cls.LENGTH_NMT)
        parsed = struct.unpack(cls.FORMAT_NMT, data)
        n_parameters, namespace, revision, redirect_index = parsed
        namespace = namespace.decode(constants.ENCODING)
        url = read_until_zero(f).decode(constants.ENCODING)
        title = read_until_zero(f).decode(constants.ENCODING)
        parameters = [read_until_zero(f).decode(constants.ENCODING) for i in range(n_parameters)]
        return cls(
            namespace,
            revision,
            redirect_index,
            url,
            title,
            parameters,
        )

    def get_disk_size(self):
        url_size = len(self.url.encode(constants.ENCODING)) + 1  # 1 for the null byte
        if self._title:
            title_size = len(self.title.encode(constants.ENCODING)) + 1  # 1 for the null byte
        else:
            title_size = 1  # for the null byte
        if self.parameters:
            # parameter handling not yet implemented
            raise NotImplementedError("get_disk_size() does not yet support parameters!")
        parameters_size = 0  # needs to be changed later
        size = BaseEntry._get_mimetype_size(self) + self.LENGTH_NMT + url_size + title_size + parameters_size
        return size

    def to_bytes(self):
        data = struct.pack(
            self.FORMAT_MT + self.FORMAT_NMT[1:],  # [1:] to get rid of endian
            self.mimetype_id,
            0,  # parameter len,
            self.namespace.encode(constants.ENCODING),
            0,  # revision,
            self.redirect_index,
        )
        data += self.url.encode(constants.ENCODING) + b"\x00"
        data += self._title.encode(constants.ENCODING) + b"\x00"
        # if any parameters were here, they would now be appended
        return data

    # ============== redirect methods ==============

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
