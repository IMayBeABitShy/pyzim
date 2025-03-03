"""
PyZim Example: search for articles.

As part of this example, we open a ZIM file for reading and search articles.
"""
import argparse

import pyzim
from pyzim.search import XapianSearch


def main():
    """
    The main function.
    """
    # start with creating an argument parser
    # this allows the user to specify arguments via command line
    # NOTE: this has nothing to do with pyzim and is purely for convenience
    # alternatively, just set zimpath and search terms to some fixed values

    parser = argparse.ArgumentParser(
        description="Search a ZIM file",
    )
    parser.add_argument("zimpath", help="path to ZIM file")
    parser.add_argument("terms", nargs="+", help="search terms")
    ns = parser.parse_args()

    zimpath = ns.zimpath
    term = " ".join(ns.terms)

    # now, let's actually start working with the ZIM file

    # first, open the ZIM file for reading
    # there are multiple ways to do this, but the easiest is to use
    # pyzim.archive.Zim.open(path) as a context manager
    # you could also directly pass a file-like object to the constructor

    with pyzim.Zim.open(zimpath) as zim:
        # now we've opened the ZIM file in read-mode.
        # it will be closed automaticall when we leave this with-context

        # Let's create a search instance
        # there are multiple ways to search a ZIM file. These are all
        # implemented as subclasses of pyzim.search.BaseSearch
        # the most relevant being:
        #  - pyzim.search.XapianSearch.open_fulltext(zim)
        #      This returns a xapian search capable of searching the
        #      entire text in the ZIM file, but requires the ZIM file
        #      to include a fulltext index as well as the xapian dependency
        #  - pyzim.search.XapianSearch.open_title(zim)
        #       Like the fulltext version, but only searches through titles
        #       This is most commonly used for suggestions and quick navigation.
        #       Most ZIM files will include a title index.
        #  - pyzim.search.TitleStartSearch
        #       A non-xapian based search for titles. It is pretty much
        #       always available and resource efficient, but is case sensitive
        #       and can only find entries starting with a term
        # alternatively, if you don't care which one you get, you can
        # simply call zim.get_search(), which will return the 'best'
        # available search
        search = XapianSearch.open_title(zim)

        # perform a search
        # the resulting SearchResultSet object can be used to iteratively
        # fetch results and get some information about the results
        results = search.search(term)

        # display the results
        # first, print the estimated number of results
        # this value may be wrong of significantly off, so don't rely on it
        print("Estimated number of results: ", results.n_estimated)
        # let's iterate over the first 20 results
        # there's also a method to only iterate in a certain range,
        # which may be useful for  implementing pagination
        print("Results:")
        for i, result in enumerate(results):
            if i >= 20:
                break
            # a SearchResult has various attributes, the most interesting ones being the the title, full_url and likelihood
            # some results may be able to provide you with an exerpt as well.
            print("  {}. '{}' at {} ({}%)".format(result.result_number, result.title, result.full_url, result.likelihood*100))

        # cleanup
        # a search object can be reused, but when we are done it should
        # be closed.
        search.close()



if __name__ == "__main__":
    main()
