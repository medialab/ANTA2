#!/usr/bin/python
# -*- coding: utf-8 -*-
# coding=utf-8

import logging
import os

from anta.util import config
from anta.annotators import pattern_annotator
from anta.annotators import tika_annotator
from anta.storage.solr_client import SOLRInterface


def read():
    data_dir = config.config["data_dir"]
    ipcc_path = data_dir + "/IPCC/"
    read_collection(ipcc_path)


def read_collection(dir_path, root_path=None):
    logging.info("ipcc_reader : Starting read_collection: {}".format(dir_path))
    if not root_path:
        root_path = dir_path
    file_names = os.listdir(dir_path)
    count = 0
    si = SOLRInterface()
    for file_name in file_names:
        file_path = os.path.join(dir_path,file_name)
        if os.path.isdir(file_path):
            logging.debug("{} is a directory".format(file_path))
            read_collection(file_path)
        else:
            if not file_name.startswith(".") and not file_name in ["Icon\r"]:
                logging.debug("{} is a file".format(file_path))
                # Text extraction
                meta, text = tika_annotator.extract_all(file_path)
                if meta and text:
                    count += 1
                    document = create_document(root_path, file_path, meta, text)
                    logging.info("document: {}".format(document))
                    # index in solr
                    si.add_document(document)
    si.commit()
    logging.info("ipcc_reader : Finishing read_collection: count: {}".format(count))


def create_document(root_path, file_path, file_meta, file_text):
    file_meta["corpus"] = "IPCC"

    # generate id
    rec_id = file_path.replace(root_path,"").replace("/","")
    last_point_index = rec_id.rfind(".")
    if last_point_index != -1:
        rec_id = rec_id[:last_point_index]
    rec_id = normalize_filename(rec_id)
    #logging.info("rec_id: {}".format(rec_id))

    file_meta["id"] = "IPCC-" + rec_id.strip()
    text = file_text.strip()
    file_meta["text"] = text
    file_meta["text_anta"] = pattern_annotator.extract_text_pos_tags(text, "en", ["NP"])

    return file_meta


def normalize_filename(filename):
    filename = filename.replace("/", "_")
    filename = filename.replace("(", "_")
    filename = filename.replace(")", "_")
    filename = filename.replace("[", "_")
    filename = filename.replace("]", "_")
    filename = filename.replace("+", "_")
    filename = filename.replace(":", "_")
    filename = filename.replace(";", "_")
    filename = filename.replace("&", "_")
    filename = filename.replace(",", "_")
    filename = filename.replace("%", "_")
    filename = filename.replace("$", "_")
    filename = filename.replace("@", "at")
    filename = filename.replace("é", "e")
    filename = filename.replace("è", "e")
    filename = filename.replace("ê", "e")
    filename = filename.replace("-", "_")
    filename = filename.replace(".", "_")
    return filename
