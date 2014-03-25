#!/usr/bin/python
# -*- coding: utf-8 -*-
# coding=utf-8

import csv
import logging

from anta.util import config
from anta.storage.solr_client import SOLRInterface


def read():
    data_dir = config.config["data_dir"]
    enb_csv_path = data_dir + "/ENB Reports/ENB Reports Complete&Cleaned.csv"
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
        # id    filename    City    ENB_ref itle    Country Year    text    nb_par  length
        csv_reader = csv.DictReader(csv_file, delimiter='\t')
        for row in csv_reader:
            document = {}
            document["corpus"] = "ENB"
            document["id"] = "ENB-" + row["id"].strip()
            if row["filename"]:
                document["file_name"] = row["filename"].strip()
            if row["City"]:
                document["city"] = row["City"].strip()
            if row["ENB_ref"]:
                document["part_volume"] = row["ENB_ref"][7:9]
                document["part_number"] = row["ENB_ref"][17:]
            if row["itle"]:
                document["title"] = row["itle"].strip()
            if row["Country"]:
                document["countries"] = [x.strip() for x in row["Country"].split(";")]
            if row["Year"]:
                document["date_year"] = row["Year"].strip()
            if row["text"]:
                document["content"] = row["text"].strip()
            if row["nb_par"]:
                document["extent_paragraphes"] = row["nb_par"].strip()
            if row["length"]:
                document["extent_words"] = row["length"].strip()
            logging.info(document)
            yield document
