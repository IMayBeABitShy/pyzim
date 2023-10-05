# pyzim - a python package for working with ZIM files

**Note:** `pyzim` is published on PyPI as `python-zim` due to a naming conflict with an existing package.

`pyzim` is a semi-pure python package for working with ZIM files. A ZIM file is basically a very highly compressed archive of a website. Examples for ZIM files include offline versions of wikipedia, stackoverflow, project gutenberg and many more.

`pyzim` aims to provide a very flexible and open method of interacting with ZIM files. For example, this project aims to give developers the choice whether they want access entries in a ZIM file as fast as possible or with as little RAM usage as possible. `pyzim` itself is written in pure python and does not depend on `libzim`. However, modern ZIM files use zstandard compression and `pyzim` depends on a C library for working with such files.

## Features

`pyzim` is nearly fully implemented. It supports nearly all reader featurs and you *should* be able to read all modern ZIM files, but some features (like search) are still missing. A writer also exists and is even capable of editing existing ZIM files.

**Basic features:**

Most read and write operations on ZIM files are implemented.

- Read and write ZIM files
- all compression types are supported (at least at the time this document is being written)
- Access header informations and metadata
- access clusters and entries directly
- iterate over entries and clusters
- edit existig ZIM files (add new entries, remove entries, edit them and clusters)
- a space allocation algorithm tries to recycle unused space in a ZIM file when it is being edited.
- work with ZIM files at a specified offset (untested)

**Missing features:**

The following features are still missing, but planned:

- search functions
- simple illustration methods (you can already read metadata illustrations, but you will have to convert them to PIL images manually)
- various additional CLI tools
- support for ZIM files without namespaces

**Additional features:**

In addition to regular ZIM functionality, the following features are also implemented:

- configurable caching of entries and clusters for better performance
- various alternative implementations of clusters for better performance at the cost of RAM
- a policy system to manage resource allocation behavior (e.g. use a policy to reduce RAM usage as much as possible at the cost of access speed)
- ZIM editing.

**General project features:**

- extensive API documentation (but not yet hosted online)
- extensive software tests (branch-coverage of 98% at the time of writing)
- examples are provided

## Installation

`pyzim` is published on PyPI as `python-zim` due to a naming conflict with an existing package.

**Via pip from PyPI**
To install via `pip`, run `pip install python-zim`. Alternatively, run `pip install python-zim[all]` to install all additional dependencies (like compression and testing libraries).

Here is a full ist of supported extra dependencies (usage: `pip install python-zim[<extra>]`):

- **all:** all extra dependencies.
- **compression:** compression related dependencies.
- **testing:** testing related dependencies. Please note that `tox` will install further dependencies during testing.

**From source**

1. Download the source code using git: `git clone https://github.com/IMayBeABitShy/pyzim.git`
2. `cd` into directory: `cd pyzim`
3. Install using `pip`: `pip install .[compression,testing]`. See above for the meaning of the extras specified. You may have to use `python3 -m pip` instead and/or specify `--user`.

## Example

Please take a look at the `examples/` directory for fully commented examples.

```python
# read a specific file from the ZIM

import argparse

import pyzim

with pyzim.Zim.open(zimpath) as zim:
    entry = zim.get_content_entry_by_url(entrypath)
    entry = entry.resolve()
    print("URL: ", entry.url)
    print("Full URL: ", entry.full_url)
    print("Redirect: ", entry.is_redirect)
    print("Title: ", entry.title)
    print("Mimetype: ", entry.mimetype)
    print("Content location: {}@{}".format(entry.blob_number, entry.cluster_number))
    print("\n\n=====CONTENT=====\n\n")
    print(entry.read())
```

## Documentation

`pyzim` is extensively documented using `pydoctor`. There is currently no online version of the documentation, but you can build it locally by running `tox -e docs` in the project directory, which will output HTML documentation to `html/apidocs/`. This requires `tox` to be installed.

If you are a contributor looking to write you own documentation, you can find a pydoctor syntax guide [here](https://pydoctor.readthedocs.io/en/latest/codedoc.html).

## Testing

At the time of writing this document, `pyzim` achieves a (statement-based) test coverage of 98%. You can run the tests locally by executing `tox` in the project directory. Specify the `testing` extra during installation of `pyzim` to automatically install all test dependencies.

`pyzim` logs a lot of low-level operations at numeric values below the `DEBUG` level. For example, each entry being read is logged, but normally aren't shown. See the documentation of `pyzim.constants` for these log levels. Editing `tox.ini` and changing the log level may be helpful when debugging.

## FAQ

**Why do I get an `UnsupportedCompressionType` exception with a ZIM file?

`pyzim` depends on other libraries to handle the decompression of data from the ZIM file. Luckily, the vast majority of these libraries come included with most python distributions. Unfortunately, these libraries may not be included when you build python yourself. Additionally, the most common compression in modern ZIM files is `zstandard`, for which `pyzim` depends on `pyzstd`. Please ensure that this library is installed.

You can automatically install all optional compression dependencies by installing the `compression` extra for `pyzim`.

**Why do I get a `BindRequired` exception / what does "bound/unbound" mean?

`pyzim` differentiates between *bound* and *unbound* entries/clusters/... . An *unbound* object is an object that is not attached to any ZIM object. By default, most objects should be automatically bound by the various methods for accessing them, but if you are accessing any class directly you may encounter unbound ones.

You can bind any such objects by calling their `.bind(zim_object)` method.

The idea behind this behavior is that we should be able to use the same code for readers and writers.

## See also

The following section lists various other resources related to ZIM files, which may be of interest to you. This includes enduser applications, alternative libraries, documentation and more. These lists are by no means exclusive.

**ZIM programming libraries and documentation**

- [libzim](https://github.com/openzim/libzim): the reference implementation of the ZIM file format
- [python-libzim](https://github.com/openzim/libzim): python bindings for `libzim`, if you want to use another ZIM module
- [ZIM file format specification](https://wiki.openzim.org/wiki/ZIM_file_format): for details about the ZIM file format

**ZIM files**

- [The kiwix Library](https://library.kiwix.org): A library of ZIM files provided by the kiwix project. It also allows you to browse ZIM files directly.

**ZIM viewers (For endusers)**

- [The kiwix website](https://www.kiwix.org/en/): Kiwix provides a wide range of ZIM viewers. Desktop and mobikle Apps exist.
- [kiwix-js](https://github.com/kiwix/kiwix-js), also available as a [PWA](https://pwa.kiwix.org/): A ZIM browser implemented in javascript for webbrowsers, available as extensions and as a PWA.
- [kiwix-tools](https://github.com/kiwix/kiwix-tools/): `kiwix-tools` contains `kiwix-serve`, a dedicated HTTP-Server for ZIM files.
