"""
PyZim Example: show ZIM file information

As part of this example, we open a ZIM file for reading and print header
and metadata.
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
        description="Print information about a ZIM file",
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

        # Let's print the header information
        # NOTE: the following method code is copied from
        # pyzim.header.Header.__str__(), meaning you can just use
        # str(zim.header) to achieve the same effect

        print("===== HEADER =====")
        template = """
Magic number: {mn}
Version: {mjv} (major) / {mnv} (minor)
UUID: {uuid}
Content: {ne} entries, {nc} clusters
Offsets:
    Directory pointer list: {upl}
    Cluster pointer list: {cpl}
    Mime type list: {mtl}
    Checksum: {cs}
Pages:
    Main: {mp}
    Layout: {lp}
        """
        s = template.format(
            mn=zim.header.magic_number,
            mjv=zim.header.major_version,
            mnv=zim.header.minor_version,
            uuid=zim.header.uuid,
            ne=zim.header.entry_count,
            nc=zim.header.cluster_count,
            upl=zim.header.url_pointer_position,
            cpl=zim.header.cluster_pointer_position,
            mtl=zim.header.mime_list_position,
            cs=zim.header.checksum_position,
            mp=(zim.header.main_page if zim.header.has_main_page else "none"),
            lp=(zim.header.layout_page if zim.header.has_layout_page else "none"),
        )
        print(s)

        # now, let's print the metadata
        print("\n\n======== METADATA ==========\n")
        # get all metadata keys
        # we set as_unicode=True to decode *most* keys and values
        # automatically. Illustrations won't be decoded.
        metadata = zim.get_metadata_dict(as_unicode=True)
        # sort it, just because.
        keys = sorted([k for k in metadata.keys()])
        for key in keys:
            value = metadata[key]
            if isinstance(value, bytes):
                value = "<binary content>"
            print(key, ": ", value)

        # other informations
        print("\n\n========== OTHER ============\n")
        print("Checksum (hex): ", zim.get_checksum().hex())



if __name__ == "__main__":
    main()
