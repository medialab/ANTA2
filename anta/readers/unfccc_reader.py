#!/usr/bin/python
# -*- coding: utf-8 -*-
# coding=utf-8

import csv
import logging
import os

from anta.util import config
from anta.annotators import pattern_annotator
from anta.annotators import tika_annotator
from anta.storage.solr_client import SOLRInterface


def read():
    data_dir = config.config["data_dir"]

    # 1991-2003
    unfccc_csv_path = data_dir + "/UNFCCC Documents/unfccc_adaptation_1991-2003.tar/unfccc_adaptation_metadatas.csv"
    unfccc_files_path = data_dir + "/UNFCCC Documents/unfccc_adaptation_1991-2003.tar/pdfs"
    read_collection(unfccc_csv_path, unfccc_files_path)

    # 1991-2012
    unfccc_csv_path = data_dir + "/UNFCCC Documents/unfccc_adaptation_1991-2012.tar/unfccc_adaptation_metadatas.csv"
    unfccc_files_path = data_dir + "/UNFCCC Documents/unfccc_adaptation_1991-2012.tar/pdfs"
    read_collection(unfccc_csv_path, unfccc_files_path)

    # 2003-2012
    unfccc_csv_path = data_dir + "/UNFCCC Documents/unfccc_adaptation_2003-2012.tar/unfccc_adaptation_metadatas.csv"
    unfccc_files_path = data_dir + "/UNFCCC Documents/unfccc_adaptation_2003-2012.tar/pdfs"
    read_collection(unfccc_csv_path, unfccc_files_path)


def read_collection(csv_path, files_path):
    logging.info("unfccc_reader : Starting read_collection: {}".format(csv_path))
    count = 0
    documents = convert_csv(csv_path)
    si = SOLRInterface()
    for document in documents:
        if "file_name" not in document:
            logging.error("Empty file_name !")
        else:
            file_path = os.path.join(files_path, document["file_name"])
            if os.path.exists(file_path):
                #logging.info("id: {}".format(document["id"]))
                meta, text = tika_annotator.extract_all(file_path)
                if meta and text:
                    count += 1
                    if "xmpTPg:NPages" in meta:
                        document["extent_pages"] = meta["xmpTPg:NPages"]
                    document["text"] = text
                    document["text_anta"] = pattern_annotator.extract_text_pos_tags(text, "en", ["NP"])

                    # index in solr
                    si.add_document(document)

            else:
                logging.error("Missing file: {}".format(file_path))
    si.commit()
    logging.info("unfccc_reader : Finishing read_collection: count: {}".format(count))


def convert_csv(csv_path):
    with open(csv_path, 'rb') as csv_file:
        # url|pdf_filename|symbol|title|authors|pdf_url|pdf_language|abstract|
        # meeting|doctype|topics|keywords|countries|pubdate|year
        csv_reader = csv.DictReader(csv_file, delimiter='|')
        for row in csv_reader:
            document = {}
            document["corpus"] = "UNFCCC"
            #logging.info("symbol: {}".format(row["symbol"]))
            if row["url"]:
                document["meta_url"] = row["url"].strip()
            if row["pdf_filename"]:
                document["file_name"] = row["pdf_filename"].strip()
            if row["symbol"]:
                document["id"] = "UN" + row["symbol"].replace("/","-").strip()
            if row["title"]:
                document["title"] = row["title"].strip()
            if row["authors"]:
                document["creators"] = [x.strip() for x in row["authors"].split(";")]
            if row["pdf_url"]:
                document["file_url"] = row["pdf_url"].strip()
            if row["pdf_language"]:
                document["language"] = row["pdf_language"].strip()
            if row["abstract"]:
                document["abstract"] = row["abstract"].strip()
            if row["meeting"]:
                document["meeting"] = row["meeting"].strip()
            if row["doctype"]:
                document["rec_type"] = row["doctype"].strip()
            if row["topics"]:
                document["topics"] = [x.strip() for x in row["topics"].split(";")]
            if row["keywords"]:
                document["keywords"] = [x.strip() for x in row["keywords"].split(";")]
            if row["countries"]:
                document["countries"] = [x.strip() for x in row["countries"].split(";")]
            if row["pubdate"]:
                document["date_issued"] = row["pubdate"].strip()
            if row["year"]:
                document["date_year"] = row["year"].strip()
            logging.info(document)
            yield document
