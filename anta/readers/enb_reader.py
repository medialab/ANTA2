#!/usr/bin/python
# -*- coding: utf-8 -*-
# coding=utf-8

import csv
import logging
import os

from anta.util import config
from anta.util import jsonbson
from anta.annotators import pattern_annotator
from anta.storage.solr_client import SOLRInterface


def read():
    data_dir = config.config["data_dir"]
    enb_csv_path = os.path.join(data_dir, "ENB", "ENB-reports.csv")
    read_collection(enb_csv_path)


def read_collection(csv_path):
    logging.info("enb_reader : Starting read_collection: {}".format(csv_path))
    count = 0
    documents = convert_csv(csv_path)
    si = SOLRInterface()
    for document in documents:
        count += 1
        # index in solr
        si.add_document(document)
    si.commit()
    logging.info("enb_reader : Finishing read_collection: count: {}".format(count))


def convert_csv(csv_path):
    with open(csv_path, 'rb') as csv_file:
        csv_reader = csv.DictReader(csv_file, delimiter=',')
        for row in csv_reader:
            text = row["text"]
            row["text_anta"] = pattern_annotator.extract_text_pos_tags(text, "en", ["NP"])
            logging.info(row)
            yield row


def test():
    data_dir = config.config["data_dir"]
    csv_path = os.path.join(data_dir, "ENB", "ENB-reports.csv")
    with open(csv_path, 'rb') as csv_file:
        csv_reader = csv.DictReader(csv_file, delimiter=',')
        for row in csv_reader:
            text = row["text"]
            result = pattern_annotator.parse_text_as_json(text, "en")
            logging.info(jsonbson.dumps_json(result, True))

