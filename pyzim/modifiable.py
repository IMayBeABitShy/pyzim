"""
Mix-in for modifiable objects in an archive.

This module contains the logic to manage "dirty"/modified states of objects.
"""
from .exceptions import NonMutable


class ModifiableMixIn(object):
    """
    A mix-in class for modifiable objects.

    This class contains the logic to mark objects as "dirty" (modified).
    It also defines an interface for getting the disk size of such objects.

    A Modifiable can have child objects ("sub-modifiables"). If any such
    object is dirty, this instance will also be considered dirty.

    This class also provides a simple check to make an object read-only,
    but this still requires L{ModifiableMixIn.ensure_mutable} to be called
    before each change.

    @ivar mutable: if not nonzero, prevent modifications of this object.
    @type mutable: L{bool}

    @ivar _dirty: a boolean flag that's nonzero if this object has been modified
    @type _dirty: L{bool}
    @ivar _submodifiables: a list of child objects, whose dirty state will affect this objects dirty state.
    @type _submodifiables: L{list} of L{ModifiableMixIn}
    @ivar _old_disk_size: the size of this object on disk before any modifications since the last flush/read
    @type _old_disk_size: L{int} or L{None}
    """
    def __init__(self):
        """
        The default constructor.

        Don't forget to call this constructor in subclasses!
        """
        self._dirty = False
        self._submodifiables = []
        self._old_disk_size = None
        self.mutable = True

    @property
    def dirty(self):
        """
        True if this object or a sub-modifiable has been modified.

        @return: True if this object or a child has been modified
        @rtype: L{bool}
        """
        return self._dirty or any([sm.dirty for sm in self._submodifiables])

    @dirty.setter
    def dirty(self, value):
        """
        Setter for L{ModifiableMixIn.dirty}

        @param value: new value to set
        @type value: L{bool}
        """
        self._dirty = value

    def mark_dirty(self):
        """
        Convenience function to mark this object as dirty.

        You can also simply set L{ModifiableMixIn.dirty} to C{True}.
        """
        self._dirty = True

    def add_submodifiable(self, child):
        """
        Add another modifiable object as a child of this one.

        @param child: modifiable object to add as an child
        @type child: L{ModifiableMixIn}
        @raises TypeError: if the passed object is not a L{ModifiableMixIn}
        """
        if not isinstance(child, ModifiableMixIn):
            raise TypeError("Expected an instance of ModifiableMixIn, not {}!".format(type(child)))
        if child not in self._submodifiables:
            self._submodifiables.append(child)

    def remove_submodifiable(self, child):
        """
        Remove a submodifiable from this object.

        @param child: child to remove
        @type child: L{ModifiableMixIn}
        @raises TypeError: if the passed object is not a L{ModifiableMixIn}
        @raises ValueError: if the submodifiable has not yet been registered
        """
        if not isinstance(child, ModifiableMixIn):
            raise TypeError("Expected an instance of ModifiableMixIn, not {}!".format(type(child)))
        if child not in self._submodifiables:
            raise ValueError("Object {} not registered as a child of this object!".format(child))
        self._submodifiables.remove(child)

    def after_flush_or_read(self):
        """
        This method should be called after this object has been read and/or
        flushed to disk. In other words, it should be called at least
        once whenever this object matches the state of the object on the
        disk.

        This method sets the old disk size, which allows us to late free
        the allocated space of the old object on disk. Thus, this method
        requires L{ModifiableMixIn.get_disk_size} to work.

        In addition, the object will be marked as non-dirty afterwards.
        """
        if self._old_disk_size is None:
            # initial read
            self._old_disk_size = self.get_initial_disk_size()
        else:
            self._old_disk_size = self.get_disk_size()
        self.dirty = False

    def get_disk_size(self):
        """
        Calculate the size of this object when written to a file.

        NOTE: in this context, size refers to the direct size of the object.
        If this object contains references to other objects, their sizes
        will not be included. For example, a L{pyzim.entry.ContentEntry}
        also links to a blob, but this function will only return the size
        of the entry itself, excluding the referenced blob.

        @return: the size, in bytes
        @rtype: L{int}
        """
        raise NotImplementedError("{}.get_disk_size() not implemented by subclass!".format(self.__class__))

    def get_unmodified_disk_size(self):
        """
        Return the size of this object when written to a file before any
        modifications has been made since the last read/flush.

        This value is thus equal to the value returned by L{ModifiableMixIn.get_disk_size}
        at the time of the last call to L{ModifiableMixIn.after_flush_or_read}.

        @return: the size in bytes before any modifications since last flush/read or L{None} if not set.
        @rtype: L{int} or L{None}
        """
        return self._old_disk_size

    def get_initial_disk_size(self):
        """
        Return the size of this object on disk as it has been read.

        This differs from L{ModifiableMixIn.get_disk_size} and
        L{ModifiableMixIn.get_unmodified_disk_size}, as both methods
        return the size this object would have it would be written. This
        is important, because sometimes we can not guarantee that an
        object has the same size it would have when we write it without
        any further modification. An example would be a L{pyzim.cluster.Cluster},
        which may have a different size due to a mismatch in configuration
        parameters even when using the same compression type.

        This method should be implemented by subclasses if the previously
        mentioned behavior is possible. By default, this just returns
        the same value as L{ModifiableMixIn.get_disk_size}.

        @return: the disk size as the object had when it was read from disk in bytes
        @rtype: L{int}
        """
        return self.get_disk_size()

    def ensure_mutable(self):
        """
        If this object is non-mutable, raise an Exception.

        @raises pyzim.exceptions.NonMutable: if C{self.mutable = False}.
        """
        if not self.mutable:
            raise NonMutable("Object {} is not mutable!".format(repr(self)))
