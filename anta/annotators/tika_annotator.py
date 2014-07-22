#!/usr/bin/python
# -*- coding: utf-8 -*-
# coding=utf-8

import logging
import mimetypes
import os
import time
import zipfile
from StringIO import StringIO

import requests

from anta.util import config

config_tika = config.config["tika"]

tika_endpoint = config_tika["endpoint"]
tika_endpoint_all = tika_endpoint + "all"
tika_endpoint_meta = tika_endpoint + "meta"
tika_endpoint_text = tika_endpoint + "tika"

# Install tika
# brew install tike
# Start the tika REST server
# tika-rest-server
# Request examples:
# curl -T test.pdf http://localhost:9998/meta
# curl -T test.pdf http://localhost:9998/tika
# curl -T test.pdf http://localhost:9998/all > tika-response.zip
# curl -X PUT -d @test.doc http://localhost:9998/tika --header "Content-Type: application/msword"


def extract_meta(file_path):
    response = request_tika(tika_endpoint_meta, file_path)
    meta = response.decode('utf-8')
    metadata = tika_to_metadata(meta)
    logging.debug("metadata: {}".format(metadata))
    return metadata


def extract_text(file_path):
    response = request_tika(tika_endpoint_text, file_path)
    text = response.decode('utf-8')
    logging.debug("text: {}".format(text))
    return text


def extract_all(file_path):
    try:
        zipdata = StringIO()
        zipdata.write(request_tika(tika_endpoint_all, file_path))
        myzipfile = zipfile.ZipFile(zipdata)
        metafile = myzipfile.open('__METADATA__')
        meta = metafile.read().decode('utf-8')
        metadata = tika_to_metadata(meta, file_path)
        logging.debug("metadata: {}".format(metadata))
        textfile = myzipfile.open('__TEXT__')
        text = textfile.read().decode('utf-8')
        logging.debug("text: {}".format(text))
        return metadata, text
    except:
        logging.error("Tika error: {}".format(file_path))
        return None, None


def request_tika(url, file_path):
    headers = {}
    headers["Content-Length"] = os.stat(file_path).st_size
    headers["Content-Type"] = mimetypes.guess_type(file_path)[0]
    headers["Date"] = time.strftime("%a, %d %b %Y %X GMT", time.gmtime())
    
    files = {'file': open(file_path, 'rb')}

    response = requests.put(url, files=files, headers=headers)
    response.encoding = "utf-8"
    return response.content


def tika_to_metadata(tika, file_path):
    # convert str to dict
    tikadict = {}
    lines = tika.split("\n")
    for line in lines:
        if line:
            linesplit = line.replace("\"","").split(",")
            if linesplit and len(linesplit) > 1:
                key = linesplit[0]
                value = linesplit[1]
                tikadict[key] = value
    return tikadict_to_metadata(tikadict, file_path)


def tikadict_to_metadata(tikadict, file_path):
    metadata = {}
    # Extract file_name
    metadata["file_name_s"] = os.path.basename(file_path)
    # Extract interesting metadata from tika meta
    # dc:title -> title
    if "dc:title" in tikadict:
        metadata["title"] = tikadict["dc:title"]
    else:
        metadata["title"] = ""
    # dcterms:created -> date
    if "dcterms:created" in tikadict:
        metadata["date"] = tikadict["dcterms:created"]
    # xmpTPg:NPages -> extent_pages
    if "xmpTPg:NPages" in tikadict:
        metadata["extent_pages_s"] = tikadict["xmpTPg:NPages"]
    return metadata
