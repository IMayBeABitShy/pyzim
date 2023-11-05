"""
Utilities for applying changes to an entire ZIM file.

@var logger: logger used by the translator
@type logger: L{logging.Logger}
"""
import logging

from .iter import iter_by_cluster
from ..archive import Zim
from ..cluster import InMemoryCluster
from ..cache import LastAccessCache, TopAccessCache
from ..item import Item
from ..policy import Policy


logger = logging.getLogger(__name__)


class ZimTranslator(object):
    """
    The ZimTranslator provides functionality to "translate" one ZIM file
    into another.

    "Translate" basically means that you can apply changes to the ZIM.
    For example, you could read a ZIM, convert all titles to upper case,
    and write them to the output ZIM.

    @ivar zim_in: the input ZIM during the translation process
    @type zim_in: L{pyzim.archive.Zim}
    @ivar zim_out: the output ZIM during the translation process
    @type zim_out: L{pyzim.archive.Zim}
    """
    def __init__(self):
        """
        The default constructor.
        """
        # NOTE: order is important
        logger.info("Opening ZIM files...")
        self.zim_in = self.open_in()
        self.zim_out = self.open_out()

    def open_in(self):
        """
        Open the input ZIM file and return it.

        This method needs to be overwritten in subclasses. It is
        recommended that the ZIM file uses some cluster caching and a
        caching cluster (e.g. L{pyzim.cluster.OffsetRememberingCluster}.

        @return: the ZIM file opened for input
        @rtype: L{pyzim.archive.Zim}
        """
        raise NotImplementedError("This method needs to be overwritten in subclasses!")

    def open_out(self):
        """
        Open the output ZIM file and return it.

        This method is guaranteed to be called only after L{ZimTranslator.open_in}
        has been called.

        This method needs to be overwritten in subclasses.

        @return: the ZIM file opened for output
        @rtype: L{pyzim.archive.Zim}
        """
        raise NotImplementedError("This method needs to be overwritten in subclasses!")

    def translate(self):
        """
        Run the translation process.
        """
        logger.info("Processing header...")
        self.handle_header()
        url_group_n = 0
        logger.info("Creating entry-cluster mapping...")
        for url_group in iter_by_cluster(self.zim_in):
            logger.info("Processing URL group {}...".format(url_group_n))
            for full_url in url_group:
                logger.debug(full_url)
                entry = self.zim_in.get_entry_by_full_url(full_url)
                if entry.is_redirect:
                    self.handle_redirect(entry)
                else:
                    item = Item.from_entry(entry)
                    new_items = self.handle_item(item)
                    if not new_items:
                        continue
                    if isinstance(new_items, Item):
                        # wrap in tuple
                        new_items = (new_items, )
                    for new_item in new_items:
                        self.zim_out.add_item(new_item)
            url_group_n += 1
        logger.info("Finalizing...")
        self.finalize()

    def handle_header(self):
        """
        Process the header.

        The ZIM input and output files are available as attributes.
        This method will be called before the content translation process
        starts. If you want to make changes to the header later on,
        you can do so both in the various C{handle_} methods as well as
        the L{ZimTranslator.finalize} method.

        You can also do other tasks during this method, like setting the
        mainpage.

        By default this method will copy the mainpage URL.
        """
        self.zim_out.set_mainpage_url(self.zim_in.get_mainpage_entry().resolve().url)

    def handle_redirect(self, entry):
        """
        Handle a redirect in the input ZIM file.

        The default implementation adds the redirect to the output ZIM file.

        @param entry: redirect entry to handle
        @type entry: L{pyzim.entry.RedirectEntry}
        """
        target = entry.follow()
        self.zim_out.add_full_url_redirect(
            entry.full_url,
            target.full_url,
            title=(entry.title if entry.title != entry.url else None),
        )

    def handle_item(self, item):
        """
        Handle an encountered item.

        If this method returns a value other than L{None}, it is presumed
        to either be an item to add or an iterable of items to add to
        the output ZIM.

        By default, this method will look for a method called
        C{handle_<namespace>} and call it with the item, returning the
        return value. If no such method exists, L{ZimTranslator.handle_default}
        will be called.

        @param item: item to process
        @type item: L{pyzim.item.Item}
        @return: item(s) to add to output ZIM or L{None}
        @rtype: L{None} or (iterable of) L{pyzim.item.Item}
        """
        namespace = item.namespace
        handler_name = "handle_" + namespace
        handler = getattr(self, handler_name, self.handle_default)
        return handler(item)

    def handle_default(self, item):
        """
        Handle an encountered item for which no namespace-specific handler
        method was defined.

        By default this will return the original item, adding it to the
        output ZIM.

        @param item: item to process
        @type item: L{pyzim.item.Item}
        @return: item(s) to add to output ZIM or L{None}
        @rtype: L{None} or (iterable of) L{pyzim.item.Item}
        """
        return item

    def handle_X(self, item):
        """
        Handle an item in the X namespace.

        The X namespace contains search indexes. As pyzim generates some
        (e.g. title lists), adding these items to the output ZIM may
        result in suboptimal or even wrong behavior. Thus, the default
        method only adds items the current pyzim implementation does not
        create on its own. The exact behavior is expected to change, so
        you should implement your own method if you want consistent
        behavior here.

        @param item: item to process
        @type item: L{pyzim.item.Item}
        @return: item(s) to add to output ZIM or L{None}
        @rtype: L{None} or (iterable of) L{pyzim.item.Item}
        """
        disallowed_urls = [
            "listing/titleOrdered/v0",
            "listing/titleOrdered/v1",
        ]
        if item.url not in disallowed_urls:
            return item
        logger.info("Not copying item X{} because pyzim will create it.".format(item.url))
        return None

    def handle_M(self, item):
        """
        Handle an item in the M namespace.

        The M namespace contains metadata, including MCounter. As pyzim
        generates some entries (like MCounter), adding these ZIMs to the
        output ZIM may result in suboptimal or even wrong behavior. Thus,
        the default method only adds items the current pyzim
        implementation does not create on its own. The exact behavior is
        expected to change, so you should implement your own method if
        you want consistent behavior here.
        """
        disallowed_urls = [
            "Counter",
        ]
        if item.url not in disallowed_urls:
            return item
        logger.info("Not copying item M{} because pyzim will create it.".format(item.url))
        return None

    def finalize(self):
        """
        Called when the translation process is finished.

        By default, this method flushes the output, then closes the input and output ZIM files.
        """
        self.zim_out.flush()
        self.zim_in.close()
        self.zim_out.close()


class PathReaderMixIn(object):
    """
    A mix-in class for L{ZimTranslator} that implements reading from path.

    If you inherit from this class in addition to the L{ZimTranslator} class,
    the translator will read the ZIM file at L{PathReaderMixIn.inpath}
    and write to L{PathReaderMixIn.outpath}.

    This class has its own constructor, which allows you to more easily
    set inpath and outpath. However, this does not call L{ZimTranslator.__init__},
    so be sure to call that translator as well!

    @cvar allow_replacement: whether overwriting existing files is allowed (default: yes)
    @type allow_replacement: L{bool}

    @ivar inpath: path to ZIM file to read from
    @type inpath: L{str}
    @ivar outpath: path to ZIM file to write to
    @type outpath: L{str}
    """

    allow_replacement = True

    def __init__(self, inpath, outpath):
        """
        The default constructor.

        @param inpath: path to ZIM file to read from
        @type inpath: L{str}
        @param outpath: path to ZIM file to write to
        @type outpath: L{str}
        """
        self.inpath = inpath
        self.outpath = outpath

    def get_in_policy(self):
        """
        Return the policy to use for the input ZIM file.

        By default this will be a ZIM file for reasonable performance.

        @return: the policy to use for the input ZIM file.
        @rtype: L{pyzim.policy.Policy}
        """
        policy = Policy(
            autoflush=False,  # no modifications to input are expected
            cluster_class=InMemoryCluster,
            cluster_cache_class=LastAccessCache,
            cluster_cache_kwargs={"max_size": 8},
            entry_cache_class=TopAccessCache,  # optimizing for path lookups
            entry_cache_kwargs={"max_size": 256},
        )
        return policy

    def get_out_policy(self):
        """
        Return the policy to use for the output ZIM file.

        By default this will be a ZIM file for a good compression.

        @return: the policy to use for the input ZIM file.
        @rtype: L{pyzim.policy.Policy}
        """
        policy = Policy(
            autoflush=True,
            cluster_class=InMemoryCluster,
            cluster_cache_class=LastAccessCache,
            cluster_cache_kwargs={"max_size": 8},
            entry_cache_class=TopAccessCache,  # optimizing for path lookups
            entry_cache_kwargs={"max_size": 256},
            truncate=True,
        )
        return policy

    def open_in(self):
        """
        Open the input ZIM based on the specified inpath.

        @return: the ZIM to read from
        @rtype: L{pyzim.archive.Zim}
        """
        return Zim.open(
            path=self.inpath,
            mode="r",
            policy=self.get_in_policy(),
        )

    def open_out(self):
        """
        Open the input ZIM based on the specified inpath.

        @return: the ZIM to read from
        @rtype: L{pyzim.archive.Zim}
        """
        return Zim.open(
            path=self.outpath,
            mode=("w" if self.allow_replacement else "x"),
            policy=self.get_out_policy(),
        )
