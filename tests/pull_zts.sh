#!/usr/bin/env bash

# Script to pull the ZIM testing suite if it is not already present

# cd to this directory
cd "${0%/*}"

# check if ZTS already exists
if [ !  -d "zim-testing-suite" ]
then
    # download the ZTS
    echo "Downloading the ZTS..." &&
    git clone https://github.com/openzim/zim-testing-suite.git &&
    echo "Download completed."
else
    echo "ZTS already present, skipping download."
fi
