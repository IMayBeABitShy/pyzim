"""
PyZim Example: iterate over entries.

As part of this example, we open a ZIM file for reading and print all
entry titles.
"""
import argparse

import pyzim


def main():
    """
    The main function.
    """
    # start with creating an argument parser
    # this allows the user to specify arguments via command line
    # NOTE: this has nothing to do with pyzim and is purely for convenience
    # alternatively, just set zimpath to some fixed value

    parser = argparse.ArgumentParser(
        description="Print all titles of entries in a ZIM file",
    )
    parser.add_argument("zimpath", help="path to ZIM file")
    ns = parser.parse_args()

    zimpath = ns.zimpath

    # now, let's actually start working with the ZIM file

    # first, open the ZIM file for reading
    # there are multiple ways to do this, but the easiest is to use
    # pyzim.archive.Zim.open(path) as a context manager
    # you could also directly pass a file-like object to the constructor

    with pyzim.Zim.open(zimpath) as zim:
        # now we've opened the ZIM file in read-mode.
        # it will be closed automaticall when we leave this with-context

        # Let's iterate over all entries
        # An entry is basically a file inside the ZIM file, although
        # technically they only contain the "metadata" of the entry
        # and a pointer to the actual content in a cluster
        # In pyzim, entries are implemented as subclasses of pyzim.entry.BaseEntry
        # There are two (important) kinds of entries: the ContentEntry
        # and the RedirectEntry, but let's focus on them later.

        # there are multiple ways to iterate over entries.
        # For this example, we use pyzim.archive.Zim.iter_entries(),
        # which iterates over all entries
        for entry in zim.iter_entries():
            # print the article title
            # Take a look at the "read.py" example or the apidocs to see
            # some of the other stuff you can do with entries
            print(entry.title)



if __name__ == "__main__":
    main()
