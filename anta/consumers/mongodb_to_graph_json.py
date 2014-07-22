#!/usr/bin/python
# -*- coding: utf-8 -*-
# coding=utf-8

import os

import pymongo

from anta.util import config

terms_col = "terms"
holds_col = "holds"

conf_db = config.config["mongodb"]
corpus = config.config["corpus"]
data_dir = config.config["data_dir"]

try:
    mongodb = pymongo.MongoClient(conf_db['host'], conf_db['port'])
except pymongo.errors.ConnectionFailure as e:
    print "ERROR: connexion failure to MongoDB : {}".format(e)

header_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), "graph_json_header.txt")

if not os.path.exists(os.path.join(data_dir, corpus)):
    os.mkdir(os.path.join(data_dir, corpus))

output_file_path = os.path.join(data_dir, corpus, "graph.json")

term_col = pymongo.Connection().db.collection

with open(header_path, "r") as header_file:
    with open(output_file_path, "w") as output_file:
        # Write header
        output_file.write(header_file.read())

        # Write nodes based on mongoDB terms
        output_file.write("  \"nodes\": [\n")
        terms_cursor = mongodb[corpus][terms_col].find({})
        terms_count = terms_cursor.count()
        for index, term in enumerate(terms_cursor):
            # starting node
            output_file.write("{")
            # term -> id
            output_file.write("\"id\":\"")
            output_file.write(term["term"].encode('UTF-8'))
            output_file.write("\",")
            # term -> label
            output_file.write("\"label\":\"")
            output_file.write(term["term"].encode('UTF-8'))
            output_file.write("\",")
            # starting attribute
            output_file.write("\"attributes\":[")
            # c -> node_attr_1 (termDocFreq)
            output_file.write("{\"attr\":\"node_attr_1\",\"val\":")
            output_file.write(str(term["c"]))
            output_file.write("},")
            # termFreqAllDocs -> node_attr_2 (termFreqAllDocs)
            output_file.write("{\"attr\":\"node_attr_2\",\"val\":")
            output_file.write(str(term["termFreqAllDocs"]))
            output_file.write("},")
            # g -> node_attr_3 (globality)
            output_file.write("{\"attr\":\"node_attr_3\",\"val\":")
            output_file.write(str(term["g"]))
            output_file.write("},")
            # l -> node_attr_4 (locality)
            output_file.write("{\"attr\":\"node_attr_4\",\"val\":")
            output_file.write(str(term["l"]))
            output_file.write("},")
            # s -> node_attr_5 (s)
            output_file.write("{\"attr\":\"node_attr_5\",\"val\":")
            output_file.write(str(term["s"]))
            output_file.write("},")
            # v -> node_attr_6 (v)
            output_file.write("{\"attr\":\"node_attr_6\",\"val\":")
            output_file.write(str(term["v"]))
            output_file.write("}")
            # ending attribute
            output_file.write("]")
            # ending node
            if index + 1 != terms_count :
                output_file.write("},\n")
            else:
                output_file.write("}\n")
        # Ending nodes array
        output_file.write("  ],\n")

        # Write edges based on holds
        output_file.write("  \"edges\": [\n")
        holds_cursor = mongodb[corpus][holds_col].find({})
        holds_count = holds_cursor.count()
        for index, hold in enumerate(holds_cursor):
            # Two edges for one hold in mongoDB
            i = hold["i"].encode('UTF-8')
            j = hold["j"].encode('UTF-8')
            hij = str(hold["hij"])
            hji = str(hold["hji"])
            # starting first edge
            output_file.write("{")
            # i¤j -> id
            output_file.write("\"id\":\"")
            output_file.write(i)
            output_file.write("¤")
            output_file.write(j)
            output_file.write("\",")
            # i -> source
            output_file.write("\"source\":\"")
            output_file.write(i)
            output_file.write("\",")
            # j -> target
            output_file.write("\"target\":\"")
            output_file.write(j)
            output_file.write("\",")
            # hij -> weight
            output_file.write("\"weight\":")
            output_file.write(hij)
            output_file.write("\",")
            # starting attribute
            output_file.write("\"attributes\":[")
            # hij -> edge_attr_1 (termDocFreq)
            output_file.write("{\"attr\":\"edge_attr_1\",\"val\":")
            output_file.write(hij)
            output_file.write("}")
            # ending attribute
            output_file.write("]")
            # ending first edge
            output_file.write("},\n")

            # starting second edge
            output_file.write("{")
            # j¤i -> id
            output_file.write("\"id\":\"")
            output_file.write(j)
            output_file.write("¤")
            output_file.write(i)
            output_file.write("\",")
            # j -> source
            output_file.write("\"source\":\"")
            output_file.write(j)
            output_file.write("\",")
            # i -> target
            output_file.write("\"target\":\"")
            output_file.write(i)
            output_file.write("\",")
            # hji -> weight
            output_file.write("\"weight\":")
            output_file.write(hji)
            output_file.write("\",")
            # starting attribute
            output_file.write("\"attributes\":[")
            # hji -> edge_attr_1 (termDocFreq)
            output_file.write("{\"attr\":\"edge_attr_1\",\"val\":")
            output_file.write(hji)
            output_file.write("}")
            # ending attribute
            output_file.write("]")
            # ending second edge
            if index + 1 != holds_count :
                output_file.write("},\n")
            else:
                output_file.write("}\n")
        # Ending edges array
        output_file.write("  ]\n")

        # Write ending curly bracket
        output_file.write("}")

