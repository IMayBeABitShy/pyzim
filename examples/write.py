"""
PyZim Example: Write an archive.

As part of this example, we open a ZIM file for writing and add a couple
of files.
"""
import argparse
import datetime

import pyzim


# the following may look intimidating, but is is basically just a black
# PNG in binary format. We are including this here as ZIMs need an image
IMAGE_DATA = b'\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR\x00\x00\x000\x00\x00\x000\x08\x02\x00\x00\x00\xd8`n\xd0\x00\x00\x00\x1dIDATx\x9c\xed\xc1\x01\x01\x00\x00\x00\x82 \xff\xafnH@\x01\x00\x00\x00\x00\x00\xc0\xaf\x01\x1b0\x00\x01>\xafn\xc4\x00\x00\x00\x00IEND\xaeB`\x82'


def main():
    """
    The main function.
    """
    # start with creating an argument parser
    # this allows the user to specify arguments via command line
    # NOTE: this has nothing to do with pyzim and is purely for convenience
    # alternatively, just set zimpath and entrypath to some fixed values

    parser = argparse.ArgumentParser(
        description="Write an example ZIM file",
    )
    parser.add_argument("zimpath", help="path to ZIM file")
    ns = parser.parse_args()

    zimpath = ns.zimpath

    # now, let's actually start working with the ZIM file

    # first, open the ZIM file for writing
    # there are multiple ways to do this, but the easiest is to use
    # pyzim.archive.Zim.open(path) as a context manager
    # you could also directly pass a file-like object to the constructor
    # there are multiple modes for opening a ZIM:
    #   - "r" for read-only
    #   - "w" for writing a ZIM
    #   - "x" like "w", but raises an exception if the path arleady exists
    #   - "a" or "u" for updating/editing a ZIM
    # when passing a file-like object directly, be sure it always supports
    # reading as well.

    with pyzim.Zim.open(zimpath, mode="w") as zim:
        # now we've opened the ZIM file in write-mode.
        # it will be closed automaticall when we leave this with-context
        # Closing the archive will also flush it.

        # there are multiple ways to write content to a ZIM file.
        # the most convenient method is to use pyzim.item.Item instances
        # and pyzim.archive.Zim.add_item(). This will automatically bundle
        # blobs in clusters and correctly set the various pointers.
        # Alternatively, you could also directly create the clusters
        # and entries.

        # When using an item, we first need to instantiate a subclass of
        # pyzim.blob.BaseBlobSource. These instances manage to content
        # of the blob we want to add. Here, we use an in-memory blob source.
        blob_source = pyzim.blob.InMemoryBlobSource("<HTML><HEADER><TITLE>Example ZIM</TITLE></HEADER><BODY><P>This is an example</P></BODY></HTML>")

        # now, let's instantiate the item
        item = pyzim.item.Item(
            namespace="C",  # namespace of the entry
            url="home.html",  # non-full url of the entry
            mimetype="text/html",  # mimetype of the content
            blob_source=blob_source,  # the content of the associated blob
            title="Welcome!",  # title of the entry
            is_article=True,  # whether this entry is an article
        )

        # next, add the item to the archive
        zim.add_item(item)
        # and make it the main page
        zim.set_mainpage_url("home.html")

        # Let's take a look at redirects
        # there are two main convenience methods for adding redirects:
        # .add_full_url_redirect, which allows redirects across namespaces
        # .add_redirect, which always assumes you are in the "C" namespace.
        # Of course, you could also simply create the redirect entry on
        # your own.
        zim.add_redirect("redirect", "home.html")

        # We are nearly done. The ZIM standard does require us to specifiy
        # some metadata values as well, so let's do this here.
        # technically, you could also just add items for the correct
        # path, but we have a slightly more convenient method.
        metadata = {
            "Name": "examplezim",
            "Title": "Example Zim",
            "Creator": "pyzim",
            "Publisher": "pyzim",
            "Date": datetime.datetime.today().strftime("%Y-%m-%d"),
            "Description": "The ZIM file from the pyzim writer example",
            "Language": "Eng",
        }
        for key, value in metadata.items():
            zim.set_metadata(key, value)  # let's ignore the mimetype for now.
        # also add the required image
        zim.set_metadata(
            "Illustration_48x48@1",
            IMAGE_DATA,
            mimetype="image/png",
        )

        # Great, we are done.
        # One last thing of note: items (as well as operations like redirects
        # depending on them) may not be written immediately. If you want
        # to ensure they are written, call .flush(). This is done automatically
        # when the ZIM is being closed, for example when we leave this
        # with context. Calling .flush() also updates the checksum.
        # By the way, pyzim will close (and thus flush) the archive should
        # an exception occur within this context.




if __name__ == "__main__":
    main()
