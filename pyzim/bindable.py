"""
Mix-in for binding objects to the Archive.

See L{pyzim.bindable.BindableMixIn} for more details.
"""

# so, we've got a problem with circular imports
# if we want to type-check the Zim object, we need to import pyzim.archive.
# However, said module imports various modules which in turn import this
# module, leaving us with a partially uninitialized module that may break
# imports due to a circular import.
# We can circumvent this if pyzim.archive is always imported before this
# module. Yet, can we truly guarantee that this is always the case?
# While a normal use will probably never import this module, they may
# import e.g. pyzim.entry before pyzim.archive, which in turn would too
# lead to the previously mentioned problem.
# Thus, currently no type-check for the ZIM archive happens.

# from . import archive
from .exceptions import AlreadyBound


class BindableMixIn(object):
    """
    A mix-in class for making objects bindable.

    PyZim distinguishes between two types of objects: bound and unbound
    ones. Both use the same classes, but only bound ones hold a
    reference to the zim archive. Consequently, only bound objects can
    access data in the ZIM file.

    Take an entry for example: The entry it self only contains the
    index of the cluster that contains the content as well as the index
    of the blob within the cluster. Unbound entries have no way to
    access the ZIM archive, thus the object can not provide a way to
    directly access the content. A bound entry, however, can provide
    an interface for accessing the content.

    So why have unbound objects anyway? To answer this question: it's
    way easier to work with unbound objects when they are not yet part
    of the ZIM file (e.g. when writing). It also makes moving entries
    between ZIM files easier.

    @ivar _zim: the bound ZIM archive or None
    @type _zim: L{pyzim.archive.Zim} or L{None}
    """
    def __init__(self, zim=None):
        """
        The default constructor. Don't forget to call this in subclasses.

        @param zim: if specified, bind this ZIM immediately.
        @type zim: L{pyzim.archive.Zim}
        """
        # assert isinstance(zim, archive.Zim)
        self._zim = zim

    @property
    def zim(self):
        """
        The bound ZIM archive, if any is bound. Otherwise None.

        @return: the bound ZIM archive or None
        @rtype: L{pyzim.archive.Zim} or L{None}
        """
        return self._zim

    @property
    def bound(self):
        """
        Whether this object is bound to a ZIM file or not.

        @return: True if bound, False otherwise
        @rtype: L{bool}
        """
        return (self._zim is not None)

    def bind(self, zim):
        """
        Bind this object to a ZIM file.

        This can be called multiple times, provided that "zim" is always
        the same L{pyzim.archive.Zim} object.

        @param zim: ZIM archive to bind to
        @type zim: L{pyzim.archive.Zim}
        @raise pyzim.exceptions.AlreadyBound: when already bound to a zim archive
        """
        # assert isinstance(zim, archive.Zim)
        if self.bound and (zim is not self.zim):
            raise AlreadyBound("Already bound to: {}".format(repr(self._zim)))
        self._zim = zim

    def unbind(self):
        """
        Unbind this object. Can be called multiple times.
        """
        self._zim = None
