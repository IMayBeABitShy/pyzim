"""
This module contains the L{Item} class, which is a helper for easily adding entries with blobs.

This behavior is inspired by the original libzim. Using items is
optional, but definitely helpful.
"""
from pyzim import constants
from pyzim.blob import BaseBlobSource, EntryBlobSource
from pyzim.entry import ContentEntry


class Item(object):
    """
    An item is a helper for simplifying the addition of new content to a ZIM.

    When adding new content to a ZIM, you would normally have to add the
    blob to a cluster, write the cluster, then create an entry and set
    the pointers correctly before writing it. This is quite useful, but
    also unneccessary complicated, which is where this class comes in.

    An Item contains all the metadata needed create and insert an entry
    and provides method that allow the
    L{pyzim.compressionstrategy.BaseCompressionStrategy} to automatically
    at it to a cluster.

    @ivar _namespace: namespace the entry should be part of
    @type _namespace: L{str}
    @ivar _url: (non-full) url of the entry
    @type _url: L{str}
    @ivar _mimetype: mimetype of the content
    @type _mimetype: L{str}
    @ivar _title: title for this entry. If L{None}, use the url instead
    @type _title: L{str} or L{None}
    @ivar _blob_source: blob source for the content of the entry
    @type _blob_source: L{pyzim.blob.BaseBlobSource}
    @ivar _is_article: if nonzero, entry should be an article
    @type _is_article: L{bool}
    """
    def __init__(
        self,
        namespace,
        url,
        mimetype,
        blob_source,
        title=None,
        is_article=False,
    ):
        """
        The default constructor.

        @param namespace: namespace the entry should be part of (usually C{'C'})
        @type namespace: L{str}
        @param url: (non-full) url of the entry
        @type url: L{str}
        @param mimetype: mimetype of the content
        @type mimetype: L{str}
        @param blob_source: blob source for the content of the entry
        @type blob_source: L{pyzim.blob.BaseBlobSource}
        @param title: title for this entry. If L{None}, use the url instead
        @type title: L{str} or L{None}
        @param is_article: if nonzero, entry should be an article
        @type is_article: L{bool}

        @raises TypeError: on invalid type
        @raises ValueError: on invalid values
        """
        self.namespace = namespace
        self.url = url
        self.mimetype = mimetype
        self.blob_source = blob_source
        self.title = title
        self.is_article = is_article

    @property
    def title(self):
        """
        The title for this entry.

        @return: the title that should be used for the entry
        @rtype: L{str}
        """
        if self._title is None:
            return self.url
        else:
            return self._title

    @title.setter
    def title(self, value):
        """
        The title for this entry.

        @param value: value to set
        @type value: L{str} or L{None}
        @raises TypeError: on invalid type
        @raises ValueError: on invalid value
        """
        if not (isinstance(value, str) or value is None):
            raise TypeError("Title must be string or None, got {} instead!".format(type(value)))
        if value is not None:
            encoded_title = value.encode(constants.ENCODING)
            if b"\x00" in encoded_title:
                raise ValueError("Title can not contain a null byte!")
        self._title = value

    @property
    def url(self):
        """
        The (non-full) url of the entry.

        @return: the (non-full) url of the entry
        @rtype: L{str}
        """
        return self._url

    @url.setter
    def url(self, value):
        """
        The (non-full) url of the entry.

        @param value: value to set
        @type value: L{str}
        @raises TypeError: on invalid type
        @raises ValueError: on invalid value
        """
        if not isinstance(value, str):
            raise TypeError("URL must be a string, got {} instead!".format(type(value)))
        elif len(value) == 0:
            raise ValueError("URL can not be empty!")
        encoded = value.encode(constants.ENCODING)
        if b"\x00" in encoded:
            raise ValueError("URL can not contain a null byte!")
        self._url = value

    @property
    def namespace(self):
        """
        The namespace the entry should be part of.

        @return: the namespace the entry should be part of
        @rtype: L{str}
        """
        return self._namespace

    @namespace.setter
    def namespace(self, value):
        """
        The namespace the entry should be part of.

        @param value: value to set
        @type value: L{str}
        @raises TypeError: on invalid type
        @raises ValueError: on invalid value
        """
        if not isinstance(value, str):
            raise TypeError("Namespace must be a string, got {} instead!".format(type(value)))
        elif len(value.encode(constants.ENCODING)) != 1:
            raise ValueError("Namespace must be a string of length 1!")
        self._namespace = value

    @property
    def mimetype(self):
        """
        The mimetype of the content.

        @return: mimetype of the content
        @rtype: L{str}
        """
        return self._mimetype

    @mimetype.setter
    def mimetype(self, value):
        """
        The mimetype of the content.

        @param value: value to set
        @type value: L{str}
        @raises TypeError: on invalid type
        @raises ValueError: on invalid value
        """
        if not isinstance(value, str):
            raise TypeError("Mimetype must be a string, got {} instead!".format(type(value)))
        elif len(value) == 0:
            raise ValueError("Mimetype can not be empty!")
        encoded = value.encode(constants.ENCODING)
        if b"\x00" in encoded:
            raise ValueError("Mimetype can not contain a null byte!")
        self._mimetype = value

    @property
    def blob_source(self):
        """
        The blob source for the content of the entry.

        @return: blob source for the content of the entry
        @rtype: L{pyzim.blob.BaseBlobSource}
        """
        return self._blob_source

    @blob_source.setter
    def blob_source(self, value):
        """
        The blob source for the content of the entry.

        @param value: value to set
        @type value: L{pyzim.blob.BaseBlobSource}
        @raises TypeError: on invalid type
        """
        if not isinstance(value, BaseBlobSource):
            raise TypeError("Blob source must be a BaseBlobSource, got {} instead!".format(type(value)))
        self._blob_source = value

    @property
    def is_article(self):
        """
        Whether the entry should be an article or not.

        @return: whether the entry should be an article or not
        @rtype: L{bool}
        """
        return self._is_article

    @is_article.setter
    def is_article(self, value):
        """
        Whether the entry should be an article or not.

        @param value: value to set
        @type value: L{bool}
        @raises TypeError: on invalid type
        """
        if not isinstance(value, bool):
            raise TypeError("is_article must be a bool, got {} instead!".format(type(value)))
        self._is_article = value

    def to_entry(self, zim):
        """
        Instantiate a new entry from the information of this item.

        Cluster and blob numbers do not have to be set. The resulting
        entry will be bound.

        @param zim: ZIM archive the entry should be part of
        @type zim: L{pyzim.archive.Zim}
        @return: an entry with this items title, url, mimetype, ...
        @rtype: L{pyzim.entry.BaseEntry}
        """
        mimetype_id = zim.mimetypelist.get_index(self.mimetype, register=True)
        entry = ContentEntry(
            mimetype=mimetype_id,
            namespace=self.namespace,
            revision=0,
            cluster_number=0,  # placeholder, will be set later
            blob_number=0,  # like cluster_number
            url=self.url,
            title=self.title,
            parameters=[],
        )
        entry.is_article = self.is_article
        entry.bind(zim)
        return entry

    @classmethod
    def from_entry(cls, entry):
        """
        Create an item from an entry.

        @param entry: entry to create item from
        @type entry: L{pyzim.entry.ContentEntry}
        @return: the created item
        @rtype: L{Item}
        @raises TypeError: on type error
        """
        if not isinstance(entry, ContentEntry):
            raise TypeError("Expected a ContentEntry, got {} instead!".format(type(entry)))
        title = (entry.title if entry.title else None)
        item = cls(
            namespace=entry.namespace,
            url=entry.url,
            mimetype=entry.mimetype,
            blob_source=EntryBlobSource(entry),
            title=title,
            is_article=entry.is_article,
        )
        return item
