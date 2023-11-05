"""
This module implements the counter, which is responsible for the C{MCounter} metadata.
"""
from . import constants, exceptions
from .processor import BaseProcessor
from .item import Item
from .blob import InMemoryBlobSource, EmptyBlobSource
from .modifiable import ModifiableMixIn


class Counter(ModifiableMixIn, BaseProcessor):
    """
    The counter keeps track of how many entries of each mimetype there is.

    Please note that the counter counts when an entry is written. If you
    are using the item interface, you may have to flush the archive first.

    @ivar counts: a dict mapping mimetype to number of occurences
    @type counts: L{dict} of L{str} -> L{int}
    """
    def __init__(self, counts=None):
        """
        The default constructor.

        @param counts: initial count of each mimetype
        @type counts: L{None} or L{dict} mapping L{str} to L{int}
        """
        BaseProcessor.__init__(self)
        ModifiableMixIn.__init__(self)
        if counts is None:
            counts = {}
        assert isinstance(counts, dict)
        self.counts = counts
        self.after_flush_or_read()

    @classmethod
    def from_string(cls, s):
        """
        Initialize a counter from a string.

        @param s: string to initialize from
        @type s: L{str} or L{bytes}
        @return: the initialized counter
        @rtype: L{Counter}
        @raises TypeError: on type error
        @raises ValueError: on invalid value
        """
        if isinstance(s, bytes):
            s = s.decode(constants.ENCODING)
        elif not isinstance(s, str):
            raise TypeError("Expected str or bytes, got {} instead!".format(type(s)))
        counts = {}
        if s:
            for element in s.split(";"):
                mt, v = element.rsplit("=", 1)
                counts[mt] = int(v)
        return cls(counts=counts)

    @classmethod
    def load_from_archive(cls, zim, how="load_or_reinit"):
        """
        Initialize the counter from an existing archive.

        The 'how' parameter specifies how the counter should be initialized.
        Supported values are:

            - C{"load"}: load from an archive, returning L{None} if not possible
            - C{"ignore"}: do not initialize counter
            - C{"reinit"}: create a new counter and count all existing entries in archive
            - C{"load_or_reinit"}: attempt to C{"load"}, falling back to C{"reinit"}, default

        @param zim: ZIM archive to initialize from
        @type zim: L{pyzim.archive.Zim}
        @param how: how to initialize the counter
        @type how: L{str}
        @return: a counter reflecting the mimetype count in the archive
        @rtype: L{Counter} or L{None}
        @raises TypeError: on invalid type
        @raises ValueError: on invalid value
        """
        if not isinstance(how, str):
            raise TypeError("Expected a string, got {} instead!".format(type(how)))
        if how not in ("load", "ignore", "reinit", "load_or_reinit"):
            raise ValueError("Unknown value '{}' for parameter 'how' of Counter.load_from_archive()!".format(how))

        if how == "ignore":
            # do not load counter
            return None
        if how in ("load", "load_or_reinit"):
            # attempt to load entry
            try:
                entry = zim.get_entry_by_full_url(constants.URL_COUNTER)
                # load counter
                data = entry.read().decode(constants.ENCODING)
                return cls.from_string(data)
            except (exceptions.EntryNotFound, exceptions.BlobNotFound, ValueError):
                # can not load entry
                # if we can only load, raise an exception
                # otherwise, we continue in another if statement
                if how in ("load", ):
                    raise exceptions.NoCounter("Failed to load counter!")
        if how in ("reinit", "load_or_reinit"):
            # no entry exists in archive
            if zim._mode == "w":
                # new zim
                counter = cls()
            else:
                # updating existing ZIM
                counter = cls()
                for entry in zim.iter_entries():
                    if not counter._should_count_entry(entry):
                        # do not include redirects or entries not in C namespace
                        continue
                    counter.increment_count(entry.mimetype)
            counter.mark_dirty()
            return counter

    def to_string(self):
        """
        Dump the values of this counter into a string.

        @return: a string that can be parsed into the counter value, compliant to the ZIM specification
        @rtype: L{str}
        """
        components = []
        for mt, n in self.counts.items():
            if n == 0:
                # do not add mimetypes with not occurences
                continue
            components.append("{}={}".format(mt, n))
        return ";".join(components)

    def increment_count(self, mimetype):
        """
        Increment the count for the specified mimetype.

        @param mimetype: mimetype to increase count of
        @type mimetype: L{str}
        @raises TypeError: on type error
        """
        if not isinstance(mimetype, str):
            raise TypeError("Expected mimetype to be a string, got {} instead!".format(type(mimetype)))
        if mimetype not in self.counts:
            self.counts[mimetype] = 1
        else:
            self.counts[mimetype] += 1
        self.mark_dirty()

    def decrement_count(self, mimetype):
        """
        Increment the count for the specified mimetype.

        @param mimetype: mimetype to increase count of
        @type mimetype: L{str}
        @raises ValueError: if current count for mimetype is not at least 1
        @raises TypeError: on type error
        """
        if not isinstance(mimetype, str):
            raise TypeError("Expected mimetype to be a string, got {} instead!".format(type(mimetype)))
        if mimetype not in self.counts:
            raise ValueError("Can not decrement count of mimetype '{}' because it has not yet been registered in the counter!".format(mimetype))
        if self.counts[mimetype] <= 0:
            raise ValueError("Can not decrement count of mimetype '{}' because current value <= 0!".format(mimetype))
        self.counts[mimetype] -= 1
        self.mark_dirty()

    def get_count(self, mimetype):
        """
        Get the count of a specific mimetype.

        @param mimetype: mimetype to Get count of
        @type mimetype: L{str}
        @raises TypeError: on type error
        """
        if not isinstance(mimetype, str):
            raise TypeError("Expected mimetype to be a string, got {} instead!".format(type(mimetype)))
        return self.counts.get(mimetype, 0)

    def _should_count_entry(self, entry):
        """
        Return True if the entry should be counted.

        @param entry: entry to count
        @type entry: L{pyzim.entry.BaseEntry}
        @return: whether the entry should be counted or not
        @rtype: L{bool}
        """
        return (not entry.is_redirect) and (entry.namespace == "C")

    # ========= BaseProcessor methods ============

    def on_install(self, zim, **kwargs):
        self.zim = zim
        if not self.zim.has_entry_for_full_url(constants.URL_COUNTER):
            # add initial placeholder for the counter if it does not yet exist
            item = Item(
                namespace=constants.URL_COUNTER[0],
                url=constants.URL_COUNTER[1:],
                mimetype="text/plain",
                blob_source=EmptyBlobSource(),
                is_article=False,
                )
            self.zim.add_item(item)

    def after_entry_write(self, **kwargs):
        # update entry mimetypes
        entry = kwargs["entry"]
        if not self._should_count_entry(entry):
            # redirects do not get counted
            return
        # increment mimetype
        mimetype = entry.mimetype
        self.increment_count(mimetype)
        if not kwargs["is_new_entry"]:
            # the entry was already counted somewhere
            # decrease that count
            old_entry = kwargs["old_entry"]
            self.decrement_count(old_entry.mimetype)

    def after_entry_remove(self, **kwargs):
        # we may have to reduce a mimetype count
        entry = kwargs["entry"]
        if not self._should_count_entry(entry):
            # redirects do not get counted
            return
        self.decrement_count(entry.mimetype)

    def after_content_flush(self, **kwargs):
        # content has been writen, write counter as well
        if self.dirty:
            blob_source = InMemoryBlobSource(self.to_string())
            entry = self.zim.get_entry_by_full_url(constants.URL_COUNTER)
            cluster = entry.set_content(blob_source)
            cluster.flush()
            self.after_flush_or_read()

    # ========== ModifiableMixIn methods ============

    def get_disk_size(self):
        return len(self.to_string())
