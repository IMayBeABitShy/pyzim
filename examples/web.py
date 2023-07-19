"""
PyZim Example: serve a ZIM via web

As part of this example, we open a ZIM file for reading and start a
bottle webserver, which allows a user to directly browse the ZIM.
The webserver itself will be kept rather minimal.

NOTE: this example requires the python bottle library to be installed.
"""
import argparse

import bottle

import pyzim
from pyzim.exceptions import EntryNotFound


class ZimWebserver(object):
    """
    A webserver serving ZIMs.

    We use a dedicated class here to avoid globals by making the ZIM
    object an attribute.

    @ivar zim: zim object to serve
    @type zim: L{pyzim.archive.Zim}
    @ivar app: the bottle object
    @type app: L{bottle.Bottle}
    """
    def __init__(self, zim):
        """
        The default constructor.

        @param zim: zim object to serve
        @type zim: L{pyzim.archive.Zim}
        """
        self.zim = zim
        self.app = bottle.Bottle()
        # setup routes
        self.app.get("/", callback=self.serve_mainpage)
        self.app.get("/<path:path>", callback=self.serve_entry)

    def serve_mainpage(self):
        """
        Serve the mainpage.
        """
        try:
            # get the mainpage entry
            entry = self.zim.get_mainpage_entry()
        except EntryNotFound:
            # the mainpage entry may not exists
            bottle.abort(404, "Main entry not found.")
        else:
            if entry.is_redirect:
                # redirect the client
                # we need to know the next entry in order to get the URL
                # thus, we follow thee entry once
                target_entry = entry.follow()
                target_url = target_entry.url
                bottle.redirect(target_url)
            else:
                # content entry
                # set the mimetype
                bottle.response.content_type = entry.mimetype
                # serve the content
                # Let's assume the mainpage always fits into RAM without
                # any problems
                return entry.read()

    def serve_entry(self, path):
        """
        Serve an entry from the ZIM file.

        @param path: path of entry to serve
        @type path: L{str}
        """
        try:
            entry = self.zim.get_content_entry_by_url(path)
        except EntryNotFound:
            # no such entry, respond with 404
            bottle.abort(404, "ZIM file does not contain an entry for path '{}'!".format(path))
        else:
            # an entry exists, it may be a redirect though
            if entry.is_redirect:
                # redirect the client
                # we need to know the next entry in order to get the URL#
                # thus, we follow thee entry once
                target_entry = entry.follow()
                target_url = target_entry.url
                print("Redirecting: ", path, " -> ", target_url)
                bottle.redirect(target_url)
            else:
                # content entry
                # serve the content
                # we use .iter_read(), as bottle allows returning generators
                # this allows us to serve larger files more RAM friendly
                # set mimetype
                bottle.response.content_type = entry.mimetype
                return entry.iter_read(buffersize=2**15)

    def start(self, interface="0.0.0.0", port=80):
        """
        Start the webserver, listen to HTTP requests.

        @param interface: interface to listen on
        @type interface: L{str}
        @param port: port to listen on
        @type port: L{int}
        """
        self.app.run(host=interface, port=port, debug=True)


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
    parser.add_argument(
        "--port",
        action="store",
        type=int,
        default=8080,
        help="port to listen on",
    )
    ns = parser.parse_args()

    zimpath = ns.zimpath
    interface = "0.0.0.0"  # all interfaces
    port = ns.port

    # now, let's actually start working with the ZIM file

    # first, open the ZIM file for reading
    # there are multiple ways to do this, but the easiest is to use
    # pyzim.archive.Zim.open(path) as a context manager
    # you could also directly pass a file-like object to the constructor

    with pyzim.Zim.open(zimpath) as zim:
        # now we've opened the ZIM file in read-mode.
        # it will be closed automaticall when we leave this with-context

        # instantiate the webserver defined above
        webserver = ZimWebserver(zim)

        # tell the user the url
        print("Now listening on http://{}:{}/".format(interface, port))

        # and lets serve http requests
        webserver.start(interface=interface, port=port)



if __name__ == "__main__":
    main()
