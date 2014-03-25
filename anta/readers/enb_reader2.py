#!/usr/bin/python
# -*- coding: utf-8 -*-
# coding=utf-8

import csv
import logging

from anta.util import config
from anta.annotators import pattern_annotator
from anta.storage.solr_client import SOLRInterface

enb_ref_date_dict = {}

def read():
    data_dir = config.config["data_dir"]
    enb_init_csv_path = data_dir + "/ENB Reports/ENB Reports Complete&Cleaned.csv"
    enb_tagged_csv_path = data_dir + "/ENB Reports/ENB Reports paragraphs tagged.csv"
    
    generate_ref_date_dict(enb_init_csv_path)
    read_collection(enb_tagged_csv_path)


def generate_ref_date_dict(enb_init_csv_path):
    with open(enb_init_csv_path, 'rb') as csv_file:
        # id    filename    City    ENB_ref itle    Country Year    text    nb_par  length
        csv_reader = csv.DictReader(csv_file, delimiter='\t')
        for row in csv_reader:
            if row["ENB_ref"] and row["Year"]:
                enb_ref_date_dict[row["ENB_ref"].strip()] = row["Year"].strip()


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
        # id ,ENB_ref ,ISItermscountries ,projection_cluster_ISItermscopindex_ISItermscopindex ,text 
        # id    filename    City    ENB_ref itle    Country Year    text    nb_par  length
        csv_reader = csv.DictReader(csv_file, delimiter=',')
        for row in csv_reader:
            document = {}
            document["corpus"] = "ENB"
            document["id"] = "ENB-" + row["id "].strip()
            if row["ENB_ref "]:
                document["part_volume"] = row["ENB_ref "][7:9]
                document["part_number"] = row["ENB_ref "][17:]
                if row["ENB_ref "] in enb_ref_date_dict:
                    document["date_year"] = enb_ref_date_dict[row["ENB_ref "]]
            if row["ISItermscountries "]:
                document["countries"] = [x.strip() for x in row["ISItermscountries "].replace("- ","").split("\n")]
            if row["projection_cluster_ISItermscopindex_ISItermscopindex "]:
                document["keywords"] = [x.strip() for x in row["projection_cluster_ISItermscopindex_ISItermscopindex "].replace("- ","").split("\n")]
            if row["text "]:
                content = row["text "].strip()
                document["content"] = content
                document["text_pos"] = pattern_annotator.extract_text_pos_tags(content, "en", ["NP"])
            logging.info(document)
            yield document
