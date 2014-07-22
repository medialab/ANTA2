#!/usr/bin/python
# -*- coding: utf-8 -*-
# coding=utf-8

import csv
import logging
import os

from anta.util import config


def export_txm():
    data_dir = config.config["data_dir"]
    input_csv_path = os.path.join(data_dir, "ENB", "ENB-reports-documents.csv")
    logging.info("Starting export_txm for file: {}".format(input_csv_path))
    output_csv_path = os.path.join(data_dir, "ENB", "ENB-reports-documents-txm.csv")
    output_txt_dir = os.path.join(data_dir, "ENB", "txt")
    if not os.path.exists(output_txt_dir):
        os.mkdir(output_txt_dir)
    with open(input_csv_path, 'rb') as input_csv_file:
        with open(output_csv_path, 'w') as output_csv_file:
            count = 0
            csv_reader = csv.DictReader(input_csv_file, delimiter=',')
            fieldnames = csv_reader.fieldnames
            csvwriter = csv.DictWriter(output_csv_file, delimiter=',', fieldnames=fieldnames)
            print fieldnames
            for document in csv_reader:
                count += 1
                # remove text from CSV
                text = document["text"]
                del document["text"]
                # create a txt file with this text
                txt_file_path = os.path.join(output_txt_dir,document["id"] + ".txt")
                with open(txt_file_path, "w") as txt_file:
                    txt_file.write(text)
                # Write row in the new CSV file
                csvwriter.writerow(document)


export_txm()
