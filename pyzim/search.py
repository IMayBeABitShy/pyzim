"""
Search and suggestion logic.

@var logger: logger used by the compression strategy
@type logger: L{logging.Logger}
"""

import os
import logging

try:
    import xapian
except ImportError:  # pragma: no cover
    xapian = None
try:
    from rigour import langs
except ImportError:  # pragma: no cover
    langs = None

from . import constants
from .exceptions import MissingDependency, ZimFeatureMissing, EntryNotFound


logger = logging.getLogger(__name__)


# ================ BASE ===================

class SearchResult(object):
    """
    A single result of a search.

    Most searches, unless they couldn't find anything, will return
    multiple of these as part of a L{SearchResultSet}.

    @ivar result_number: 1-based index of this result in the result set
    @type result_number: L{int}
    @ivar likelihood: a 0-1 value describing the match rate
    @type likelihood: L{float}
    @ivar full_url: the full url of the entry this result is for
    @type full_url: L{str}
    @ivar title: title of the entry this result is for
    @type title: L{str}
    @ivar excerpt: a short excerpt/summary, provided it is available
    @type excerpt: L{str} or L{None}
    """
    def __init__(self, result_number, likelihood, full_url, title, excerpt=None):
        """
        The default constructor.

        @param result_number: 1-based index of this result in the result set
        @type result_number: L{int}
        @param likelihood: a 0-1 value describing the match rate
        @type likelihood: L{float}
        @param full_url: the full url of the entry this result is for
        @type full_url: L{str}
        @param title: title of the entry this result is for
        @type title: L{str}
        @param excerpt: a short excerpt/summary, provided it is available
        @type excerpt: L{str} or L{None}
        """
        assert isinstance(result_number, int) and result_number >= 1
        assert isinstance(likelihood, float) and (likelihood >= 0) and (likelihood <= 1)
        assert isinstance(full_url, str) and full_url
        assert isinstance(title, str)
        assert isinstance(excerpt, str) or (excerpt is None)

        self.result_number = result_number
        self.likelihood = likelihood
        self.full_url = full_url
        self.title = title
        self.excerpt = excerpt


class SearchResultSet(object):
    """
    A SearchResultSet contains the L{SearchResult}s of a search as well
    as some result metadata like the amount of matches.

    The main purpose of this class is to provide an efficient search
    that may automatically fetch more results.

    @ivar search: the search object that produced this search result
    @type search: L{BaseSearch}
    @ivar term: the used search term
    @type term: L{str}
    @ivar n_estimated: estimated number of results if available
    @type n_estimated: L{int} or L{None}
    """
    def __init__(self, search, term, n_estimated):
        """
        The default constructor.

        @param search: the search object that produced this search result
        @type search: L{BaseSearch}
        @param term: the used search term
        @type term: L{str}
        @param n_estimated: estimated number of results if available
        @type n_estimated: L{int} or L{None}
        """
        assert isinstance(search, BaseSearch)
        assert isinstance(term, str)
        assert isinstance(n_estimated, int) or (n_estimated is None)
        self.search = search
        self.term = term
        self.n_estimated = n_estimated

    def __iter__(self):
        """
        Iterate over the search results in this set.

        @return: an interator/generator yielding the search results
        @rtype: L{SearchResult}
        """
        cur_pos = 1
        while True:
            results = self.search.search_in_range(term=self.term, start=cur_pos)
            if not results:
                # no further data
                break
            for result in results:
                cur_pos += 1
                yield result

    def iter_in_range(self, start, n=20):
        """
        Iterate over the search results in this set.

        @param start: start (1-based index) of the results to find
        @type start: L{int}
        @param n: number of results to fetch
        @type n: L{int}
        @return: an interator/generator yielding the search results
        @rtype: L{SearchResult}
        """
        cur_pos = start
        while cur_pos < start + n:
            results = self.search.search_in_range(term=self.term, start=cur_pos, n=n)
            if not results:
                # no further data
                break
            for result in results:
                cur_pos += 1
                yield result

    def __len__(self):
        """
        Return an estimated amount of results.

        @return: the estimated amount of results of the search
        @rtype: L{int}
        """
        return self.n_estimated


class BaseSearch(object):
    """
    Base class for search and suggestion implementations.

    @ivar zim: the ZIM archive this search is for
    @type zim: L{pyzim.archive.Zim}
    """
    def __init__(self, zim):
        """
        The default constructor.

        @param zim: the ZIM archive this search is for
        @type zim: L{pyzim.archive.Zim}
        """
        self.zim = zim

    def close(self):
        """
        Perform any necessary cleanup.

        This should be called before the instance is dismissed.
        """
        pass

    def search(self, term):
        """
        Perform a search.

        @param term: term to search for
        @type term: L{str}
        @return: an object containing the search results
        @rtype: L{SearchResultSet}
        """
        raise NotImplementedError("This method needs to be overwritten in child classes!")  # pragma: no cover

    def search_in_range(self, term, start=None, n=20):
        """
        Perform a range-limited search.

        This function is primarily a helper function used by L{SearchResultSet}
        to iteratively fetch new results, but it can still be used independently.
        Just beware of the different return type.

        @param term: term to search for
        @type term: L{str}
        @param start: start (1-based index) of the results to find
        @type start: L{int}
        @param n: number of results to fetch
        @type n: L{int}
        @return: an object containing the search results
        @rtype: L{list} of L{SearchResult}
        """
        raise NotImplementedError("This method needs to be overwritten in child classes!")  # pragma: no cover


# ================ IMPLEMENTATIONS ===================


class TitleStartSearch(BaseSearch):
    """
    A search for finding entries whose titles start with a certain prefix.

    This search is case sensitive!
    """
    def __init__(self, *args, search_nonarticles=False, **kwargs):
        """
        The default constructor.

        All arguments that are not specific for this method will be passed to the superclass.

        @param search_nonarticles: if nonzero, search all entries and not only the articles. In this case, the search terms must begin with the namespace.
        @type search_nonarticles: L{bool}
        """
        BaseSearch.__init__(self, *args, **kwargs)

        if search_nonarticles:
            if self.zim._entry_title_pointer_list is None:
                raise ZimFeatureMissing("Zim does not contain an entry title pointer list!")
            self._titlepointerlist = self.zim._entry_title_pointer_list
        else:
            self._titlepointerlist = self.zim._article_title_pointer_list

    def search(self, term):
        # get estimate the amount of results
        if term:
            first = self._titlepointerlist.find_first_greater_equals(term)
            next = term[:-1] + chr(ord(term[-1]) + 1)
            end = self._titlepointerlist.find_first_greater_equals(next)
            estimated = end - first
        else:
            estimated = len(self._titlepointerlist)
        return SearchResultSet(search=self, term=term, n_estimated=estimated)

    def search_in_range(self, term, start=None, n=20):
        if term:
            first = self._titlepointerlist.find_first_greater_equals(term)
            next = term[:-1] + chr(ord(term[-1]) + 1)
            end = self._titlepointerlist.find_first_greater_equals(next)
        else:
            first = 0
            end = len(self._titlepointerlist)

        start_pos = first + (start - 1)
        max_n = min(n, end - start_pos)
        results = []
        for i in range(start_pos, start_pos+max_n):
            pointer = self._titlepointerlist.get_by_index(i)
            entry = self.zim.get_entry_by_url_index(pointer)
            # rudimentary likelihood calculation based on the remaining length
            try:
                likelihood = len(term) / len(entry.title)
            except ZeroDivisionError:  # pragma: no cover
                likelihood = 0
            result = SearchResult(
                result_number=(i - start_pos + 1),
                likelihood=likelihood,
                full_url=entry.full_url,
                title=entry.title,
                excerpt=None,
            )
            results.append(result)
        return results


class XapianSearch(BaseSearch):
    """
    A search for finding entries using a xapian database.
    """
    def __init__(self, *args, full_url="Xfulltext/xapian", **kwargs):
        """
        The default constructor.

        All arguments that are not specific for this method will be passed to the superclass.

        @param full_url: full URL of xapian database to use
        @type full_url: L{str}
        @raises pyzim.exceptions.ZimFeatureMissing: when the ZIM files does not include a xapian index at the specified full url
        @raises pyzim.exceptions.MissingDependency: when the xapian library could not be imported
        """

        if xapian is None:
            raise MissingDependency("Dependency xapian is required when using the xapian search!")
        if langs is None:
            raise MissingDependency("Dependency rigour is required when using the xapian search!")

        BaseSearch.__init__(self, *args, **kwargs)
        self._full_url = full_url

        self._fd = os.dup(self.zim._f.fileno())
        os.lseek(self._fd, self._offset, os.SEEK_SET)
        self._database = xapian.Database(self._fd)
        self._queryparser = xapian.QueryParser()
        self._queryparser.set_database(self._database)
        language = self._database.get_metadata("language")
        if language is None:
            logger.debug("Xapian datbase does not contain any language metadata, falling back to ZIM metadata.")
            language = self.zim.get_metadata("Language")
        if language is not None:
            # we need to convert the language
            language = langs.iso_639_alpha2(language)
            try:
                self._stemmer = xapian.Stem(language)
            except xapian.InvalidArgumentError:  # pragma: no cover
                logger.info("Failed to setup xapian stemmer!")
            else:
                self._queryparser.set_stemmer(self._stemmer)
                self._queryparser.set_stemming_strategy(xapian.QueryParser.STEM_SOME)
        else:
            logger.debug("Could not find language to use for xapian stemming, disabling it..")
            self._stemmer = None

    @classmethod
    def open_title(cls, *args, **kwargs):
        """
        Open the xapian title index.

        @param args: additional arguments that will be passed to L{XapianSearch.__init__}
        @type args: L{tuple}
        @param kwargs: additional keyword arguments that will be passed to L{XapianSearch.__init__}
        @type kwargs: L{dict}
        @return: a xapian search using the title index of a ZIM
        @rtype: L{XapianSearch}
        @raises pyzim.exceptions.ZimFeatureMissing: when the ZIM files does not include a xapian title index
        @raises pyzim.exceptions.MissingDependency: when the xapian library could not be imported
        """
        return cls(*args, full_url=constants.URL_XAPIAN_TITLE_INDEX, **kwargs)

    @classmethod
    def open_fulltext(cls, *args, **kwargs):
        """
        Open the xapian fulltext index.

        @param args: additional arguments that will be passed to L{XapianSearch.__init__}
        @type args: L{tuple}
        @param kwargs: additional keyword arguments that will be passed to L{XapianSearch.__init__}
        @type kwargs: L{dict}
        @return: a xapian search using the fulltext index of a ZIM
        @rtype: L{XapianSearch}
        @raises pyzim.exceptions.ZimFeatureMissing: when the ZIM files does not include a xapian title index
        @raises pyzim.exceptions.MissingDependency: when the xapian library could not be imported
        """
        return cls(*args, full_url=constants.URL_XAPIAN_FULLTEXT_INDEX, **kwargs)

    @property
    def _offset(self):
        """
        Return the offset of the database in the ZIM file

        @return: the offset of the xapian database in the ZIM file
        @rtype: L{int}
        """
        try:
            db_entry = self.zim.get_entry_by_full_url(self._full_url)
        except EntryNotFound as e:
            raise ZimFeatureMissing("No xapian search index for {}".format(self._full_url)) from e
        cluster_index, blob_index = db_entry.cluster_number, db_entry.blob_number
        cluster = self.zim.get_cluster_by_index(cluster_index)
        cluster_offset = cluster.offset
        blob_offset = cluster.get_offset(blob_index)
        total_offset = cluster_offset + 1 + blob_offset
        return total_offset

    def close(self):
        self._database.close()

    def search(self, term):
        query = self._queryparser.parse_query(term)
        enquire = xapian.Enquire(self._database)
        enquire.set_query(query)
        matches = enquire.get_mset(0, 10)
        estimated = matches.get_matches_estimated()
        return SearchResultSet(search=self, term=term, n_estimated=estimated)

    def search_in_range(self, term, start=None, n=20):
        if start is None:
            start = 1
        query = self._queryparser.parse_query(term)
        enquire = xapian.Enquire(self._database)
        enquire.set_query(query)
        matches = enquire.get_mset(start-1, n)
        results = []
        for m in matches:
            full_url = m.document.get_data().decode(constants.ENCODING)
            full_url = full_url[0] + full_url[2:]  # TODO: we need to change how full URLs work
            entry = self.zim.get_entry_by_full_url(full_url)
            result = SearchResult(
                result_number=m.rank+1,
                likelihood=m.percent/100,
                title=entry.title,
                full_url=full_url,
                # TODO: excerpt
            )
            results.append(result)
        return results


# test code
if __name__ == "__main__":  # pragma: no cover
    import argparse
    from .archive import Zim
    parser = argparse.ArgumentParser("Perform a search in a ZIM")
    parser.add_argument("path", action="store", help="path to ZIM file to search")
    parser.add_argument("terms", nargs="+", help="search terms")
    ns = parser.parse_args()
    term = " ".join(ns.terms)

    with Zim.open(ns.path, mode="r") as zim:
        search = TitleStartSearch(zim, search_nonarticles=True)
        # search = XapianSearch.open_title(zim)
        results = search.search(term)
        print("Estimated number of results: ", results.n_estimated)
        print("Results:")
        for i, result in enumerate(results):
            if i >= 20:
                break
            print("  {}. '{}' at {} ({}%)".format(result.result_number, result.title, result.full_url, result.likelihood*100))
        search.close()
