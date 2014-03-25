#!/usr/bin/python
# -*- coding: utf-8 -*-
# coding=utf-8

import csv
import logging

from dateutil import parser

from anta.util import config
from anta.annotators import pattern_annotator
from anta.storage.solr_client import SOLRInterface


def read():
    data_dir = config.config["data_dir"]
    nyt_csv_path = data_dir + "/NYTimes articles/nyt_articles_1990-2013_climatechangeORglobalwarming_someFields.csv"
    read_collection(nyt_csv_path)


def read_collection(csv_path):
    logging.info("nyt_reader : Starting read_collection: {}".format(csv_path))
    count = 0
    documents = convert_csv(csv_path)
    si = SOLRInterface()
    for document in documents:
        count += 1
        # index in solr
        si.add_document(document)
    si.commit()
    logging.info("nyt_reader : Finishing read_collection: count: {}".format(count))


def convert_csv(csv_path):
    with open(csv_path, 'rb') as csv_file:
        # url;snippet;lead_paragraph;abstract;source;headline;section_name;document_type;type_of_material;keywords;keys;date
        
        csv_reader = csv.DictReader(csv_file, delimiter=';')
        for row in csv_reader:
            #logging.debug("csv row: {}".format(row))
            document = {}
            document["corpus"] = "NYT"
            document["id"] = "NYT-" + row["url"]
            if row["url"]:
                document["file_url"] = row["url"]
            # content : abstract, snippet, lead_paragraph or not
            if row["abstract"]:
                document["content"] = row["abstract"]
            elif row["snippet"]:
                document["content"] = row["snippet"]
            elif row["lead_paragraph"]:
                document["content"] = row["lead_paragraph"]
                document["text_pos"] = pattern_annotator.extract_text_pos_tags(row["lead_paragraph"], "en", ["NP"])
            else:
                continue
            if row["source"]:
                document["source"] = row["source"]
            if row["headline"]:
                document["title"] = row["headline"]
            if row["section_name"]:
                document["part_name"] = row["section_name"]
            if row["document_type"]:
                document["rec_type"] = row["document_type"]
            if row["keywords"]:
                document["keywords"] = [x.strip() for x in row["keywords"].split("|")]
            if row["date"]:
                date_formatted = format_iso8601(row["date"])
                if date_formatted:
                    document["date_issued"] = date_formatted
                    document["date_year"] = date_formatted[:4]
                    document["date_year_month"] = date_formatted[:7]
            logging.info(document)
            yield document


def format_iso8601(datestr):
    try:
        #logging.debug(datestr)
        datestr = datestr.strip().split("\n")[0]
        datepy = parser.parse(datestr)

        result = []
        result.append(str(datepy.year))
        if datepy.month:
            result.append("-")
            month = str(datepy.month)
            if len(month) == 1:
                result.append("0")
            result.append(month)
            if datepy.day:
                result.append("-")
                day = str(datepy.day)
                if len(day) == 1:
                    result.append("0")
                result.append(day)
        final = "".join(result)
        #logging.debug(datestr)
        return final
    except:
        return None
