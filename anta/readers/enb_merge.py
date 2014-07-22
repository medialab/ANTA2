#!/usr/bin/python
# -*- coding: utf-8 -*-
# coding=utf-8

import csv
import os

from anta.util import config


def merge(with_txt=False):
    data_dir = config.config["data_dir"]

    enb_csv_init_path = os.path.join(data_dir, "ENB", "ENB Reports Complete&Cleaned.csv")
    enb_csv_tagged_path = os.path.join(data_dir, "ENB", "ENB Reports paragraphs tagged.csv")
    enb_result_csv_path = os.path.join(data_dir, "ENB", "ENB-reports.csv")
    enb_result_txt_path = os.path.join(data_dir, "ENB" , "txt")

    results = {}

    convert_init_csv(enb_csv_init_path, results)
    convert_tagged_csv(enb_csv_tagged_path, results)

    if with_txt:
        export_csv_txt(enb_result_csv_path, enb_result_txt_path, results)
    else:
        export_csv(enb_result_csv_path, results)


def convert_init_csv(enb_csv_init_path, results):
    with open(enb_csv_init_path, 'rb') as enb_csv_init:
        # id    filename    City    ENB_ref itle    Country Year    text    nb_par  length
        csv_reader = csv.DictReader(enb_csv_init, delimiter='\t')
        for row in csv_reader:
            document = {}
            document["corpus"] = "ENB"
            # id : "ENB-" + ENB_ref
            document["id"] = "ENB-" + row["ENB_ref"].strip().lower().replace(" ", "-")
            if row["filename"]:
                document["file_name_s"] = row["filename"].strip()
            if row["City"]:
                document["event_place_s"] = row["City"].strip()
            if row["ENB_ref"]:
                document["part_volume_s"] = row["ENB_ref"][7:9]
                document["part_number_s"] = row["ENB_ref"][17:]
            if row["itle"]:
                document["title"] = row["itle"].strip()
            if row["Country"]:
                document["event_country_s"] = row["Country"].strip()
            if row["Year"]:
                document["date"] = row["Year"].strip()
            if row["text"]:
                document["text"] = row["text"].strip()
            if row["nb_par"]:
                document["extent_paragraphes_i"] = row["nb_par"].strip()
            if row["length"]:
                document["extent_words_i"] = row["length"].strip()
            #logging.info(document)

            # merge in dict with id as the key
            if document["id"] not in results:
                results[document["id"]] = document
            else:
                text = results[document["id"]]["text"]
                text += "\n\n"
                text += document["text"]
                results[document["id"]]["text"] = text


def convert_tagged_csv(enb_csv_tagged_path, results):
    with open(enb_csv_tagged_path, 'rb') as enb_csv_tagged:
        # id ,ENB_ref ,ISItermscountries ,projection_cluster_ISItermscopindex_ISItermscopindex ,text 
        csv_reader = csv.DictReader(enb_csv_tagged, delimiter=',')
        for row in csv_reader:
            # id : "ENB-" + ENB_ref
            rec_id = "ENB-" + row["ENB_ref "].strip().lower().replace(" ", "-")
            if row["ISItermscountries "]:
                countries = {}
                if "countries_s" in results[rec_id]:
                    countries = results[rec_id]["countries_s"]
                for country in row["ISItermscountries "].replace("- ","").split("\n"):
                    countries[country.strip()] = ""
                results[rec_id]["countries_s"] = countries
            if row["projection_cluster_ISItermscopindex_ISItermscopindex "]:
                keywords = {}
                if "keywords_s" in results[rec_id]:
                    keywords = results[rec_id]["keywords_s"]
                for keyword in row["projection_cluster_ISItermscopindex_ISItermscopindex "].replace("- ","").split("\n"):
                    keywords[keyword.strip()] = ""
                results[rec_id]["keywords_s"] = keywords


def export_csv(enb_result_csv_path, results):
    with open(enb_result_csv_path, "wb") as csv_file:
        fieldnames = ["id", "corpus", "file_name_s", "part_volume_s", "part_number_s", "date", "title", "event_place_s", "event_country_s", "extent_paragraphes_i", "extent_words_i", 
                      "text", "countries_s", "keywords_s"]
        csvwriter = csv.DictWriter(csv_file, delimiter=',', fieldnames=fieldnames)
        csvwriter.writeheader()
        for key in sorted(results.iterkeys()):
            row = results[key]
            row["countries_s"] = sorted(row["countries_s"].keys())
            row["keywords_s"] = sorted(row["keywords_s"].keys())
            csvwriter.writerow(row)



def export_csv_txt(enb_result_csv_path, enb_result_txt_path, results):
    with open(enb_result_csv_path, "wb") as csv_file:
        fieldnames = ["id", "corpus", "file_name_s", "part_volume_s", "part_number_s", "date", "title", "event_place_s", "event_country_s", "extent_paragraphes_i", "extent_words_i"]
        csvwriter = csv.DictWriter(csv_file, delimiter=',', fieldnames=fieldnames)
        csvwriter.writeheader()
        for key in sorted(results.iterkeys()):
            row = results[key]
            # write txt
            text = row["text"]
            text_file_path = os.path.join(enb_result_txt_path, row["id"] + ".txt")
            with open(text_file_path, "wb") as content_file:
                content_file.write(text)
            # remove unwanted key in CSV
            del row["countries_s"]
            del row["keywords_s"]
            del row["text"]
            csvwriter.writerow(row)

merge()

