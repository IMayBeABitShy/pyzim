"""
PyZim Example: read a specific entry.

As part of this example, we open a ZIM file for reading and read an
article by URL.
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
    # alternatively, just set zimpath and entrypath to some fixed values

    parser = argparse.ArgumentParser(
        description="Read a (content) entry from a ZIM file",
    )
    parser.add_argument("zimpath", help="path to ZIM file")
    parser.add_argument("entrypath", help="path to article ion ZIM file")
    ns = parser.parse_args()

    zimpath = ns.zimpath
    entrypath = ns.entrypath

    # now, let's actually start working with the ZIM file

    # first, open the ZIM file for reading
    # there are multiple ways to do this, but the easiest is to use
    # pyzim.archive.Zim.open(path) as a context manager
    # you could also directly pass a file-like object to the constructor

    with pyzim.Zim.open(zimpath) as zim:
        # now we've opened the ZIM file in read-mode.
        # it will be closed automaticall when we leave this with-context

        # Let's get the entry
        # An entry is basically a file inside the ZIM file, although
        # technically they only contain the "metadata" of the entry
        # and a pointer to the actual content in a cluster
        # In pyzim, entries are implemented as subclasses of pyzim.entry.BaseEntry
        # There are two (important) kinds of entries: the ContentEntry
        # and the RedirectEntry, but let's focus on them later.

        # there are multiple ways to get an entry from the archive, but
        # the simplest one is pyzim.archive.Zim.get_content_entry_by_url.
        # PLEASE NOTE the "content" in the method name. ZIM files differentiate
        # between multiple namespaces, which have their own URL/path namespace.
        # Usually, you want to use the "C" namespace, which is simplified
        # by the beforementioned function.
        entry = zim.get_content_entry_by_url(entrypath)

        # The entry may be a redirect entry.
        # We can use the .redirect() method to ensure we always resolve
        # redirects
        entry = entry.resolve()

        # Let's print the entry metadata:
        print("URL: ", entry.url)
        print("Full URL: ", entry.full_url)
        print("Redirect: ", entry.is_redirect)
        print("Title: ", entry.title)
        print("Mimetype: ", entry.mimetype)
        print("Content location: {}@{}".format(entry.blob_number, entry.cluster_number))
        print("Size: ", entry.get_size())

        # now, let us print the content of the entry
        # you *could* use the cluster number to get the cluster and the
        # blob number to get the actual content of the entry
        # However, this operation is so common that it's more intuitively
        # available as the .read() method of the entry
        content = entry.read()
        # alternatively, you could use .read_iter() if you are more concerned
        # about RAM usage

        # print a couple of newlines before printing the content
        print("\n\n=====CONTENT=====\n\n")
        print(content)



if __name__ == "__main__":
    main()
