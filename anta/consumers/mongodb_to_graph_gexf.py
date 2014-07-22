#!/usr/bin/python
# -*- coding: utf-8 -*-
# coding=utf-8

import os
import datetime

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

header_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), "graph_gexf_header.txt")

if not os.path.exists(os.path.join(data_dir, corpus)):
    os.mkdir(os.path.join(data_dir, corpus))

output_file_path = os.path.join(data_dir, corpus, "graph.gexf.xml")

term_col = pymongo.Connection().db.collection

with open(header_path, "r") as header_file:
    with open(output_file_path, "w") as output_file:
        # header
        output_file.write(header_file.read())
        # meta
        output_file.write("<meta lastmodifieddate=\"")
        output_file.write(datetime.datetime.now().strftime('%Y-%m-%d'))
        output_file.write("\">\n")
        output_file.write("<creator>ANTA2</creator>\n")
        output_file.write("<description>Directed graph</description>\n")
        output_file.write("</meta>\n")
        # graph begin
        output_file.write("<graph mode=\"static\" defaultedgetype=\"directed\">\n")
        # attributes node
        output_file.write("<attributes class=\"node\">\n")
        output_file.write("<attribute id=\"node_attr_1\" title=\"termDocFreq\" type=\"integer\"/>\n")
        output_file.write("<attribute id=\"node_attr_2\" title=\"termFreqAllDocs\" type=\"integer\"/>\n")
        output_file.write("<attribute id=\"node_attr_3\" title=\"globality\" type=\"double\"/>\n")
        output_file.write("<attribute id=\"node_attr_4\" title=\"locality\" type=\"double\"/>\n")
        output_file.write("<attribute id=\"node_attr_5\" title=\"s\" type=\"double\"/>\n")
        output_file.write("<attribute id=\"node_attr_6\" title=\"v\" type=\"double\"/>\n")
        output_file.write("</attributes>\n")
        # attributes node
        output_file.write("<attributes class=\"edge\">\n")
        output_file.write("<attribute id=\"edge_attr_1\" title=\"h\" type=\"double\"/>\n")
        output_file.write("<attribute id=\"edge_attr_2\" title=\"c\" type=\"double\"/>\n")
        output_file.write("</attributes>\n")
        # Write nodes based on mongoDB terms
        output_file.write("<nodes>\n")
        terms_cursor = mongodb[corpus][terms_col].find({})
        terms_count = terms_cursor.count()
        for index, term in enumerate(terms_cursor):
            # starting node
            output_file.write("<node id=\"")
            # term -> id
            output_file.write(term["term"].encode('UTF-8'))
            # term -> label
            output_file.write("\" label=\"")
            output_file.write(term["term"].encode('UTF-8'))
            # starting attribute
            output_file.write("\"><attvalues><attvalue for=\"node_attr_1\" value=\"")
            # c -> node_attr_1 (termDocFreq)
            output_file.write(str(term["c"]))
            # termFreqAllDocs -> node_attr_2 (termFreqAllDocs)
            output_file.write("\"/><attvalue for=\"node_attr_2\" value=\"")
            output_file.write(str(term["termFreqAllDocs"]))
            # g -> node_attr_3 (globality)
            output_file.write("\"/><attvalue for=\"node_attr_3\" value=\"")
            output_file.write(str(term["g"]))
            # l -> node_attr_4 (locality)
            output_file.write("\"/><attvalue for=\"node_attr_4\" value=\"")
            output_file.write(str(term["l"]))
            # s -> node_attr_5 (s)
            output_file.write("\"/><attvalue for=\"node_attr_5\" value=\"")
            output_file.write(str(term["s"]))
            # v -> node_attr_6 (v)
            output_file.write("\"/><attvalue for=\"node_attr_6\" value=\"")
            output_file.write(str(term["v"]))
            # ending attribute and node
            output_file.write("\"/></attvalues></node>\n")
        # Ending nodes array
        output_file.write("</nodes>\n")

        # Write edges based on holds
        output_file.write("<edges>\n")
        holds_cursor = mongodb[corpus][holds_col].find({})
        holds_count = holds_cursor.count()
        for index, hold in enumerate(holds_cursor):
            # Two edges for one hold in mongoDB
            i = hold["i"].encode('UTF-8')
            j = hold["j"].encode('UTF-8')
            hij = str(hold["hij"])
            hji = str(hold["hji"])
            # starting first edge
            output_file.write("<edge id=\"")
            # i¤j -> id
            output_file.write(i)
            output_file.write("¤")
            output_file.write(j)
            # i -> source
            output_file.write("\" source=\"")
            output_file.write(i)
            # j -> target
            output_file.write("\" target=\"")
            output_file.write(j)
            # hij -> weight
            output_file.write("\"><attvalues><attvalue for=\"node_attr_1\" value=\"")
            output_file.write(hij)
            # ending attribute and edge
            output_file.write("\"/></attvalues></edge>\n")

            # starting second edge
            output_file.write("<edge id=\"")
            # j¤i -> id
            output_file.write(j)
            output_file.write("¤")
            output_file.write(i)
            # j -> source
            output_file.write("\" source=\"")
            output_file.write(j)
            # i -> target
            output_file.write("\" target=\"")
            output_file.write(i)
            # hji -> weight
            output_file.write("\"><attvalues><attvalue for=\"node_attr_1\" value=\"")
            output_file.write(hji)
            # ending attribute and edge
            output_file.write("\"/></attvalues></edge>\n")
            
        # Ending nodes array
        output_file.write("</edges>\n")

        # graph end
        output_file.write("</graph>\n")
        # gexf end
        output_file.write("</gexf>\n")