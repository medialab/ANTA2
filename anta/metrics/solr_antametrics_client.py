#!/usr/bin/python
# -*- coding: utf-8 -*-
# coding=utf-8

import logging
import xml.etree.ElementTree as ET
import os

import requests

from anta.util import console
from anta.util import config
from anta.util import jsonbson

console.setup_console()
config_solr = config.config["solr"]
solr_endpoint = config_solr["endpoint"] + "antametrics"

# curl "http://localhost:8983/solr/anta/antametrics?q=*:*&wt=xml&rows=0&metrics=tfidf"
# curl "http://localhost:8983/solr/anta/antametrics?q=*:*&wt=xml&rows=0&metrics=gl"

def extract_solr_metrics(metrics_param):
    params = {}
    params["q"] = "*:*"
    params["wt"] = "xml"
    params["rows"] = "0"
    params["fl"] = "id"
    params["metrics"] = metrics_param

    response = requests.get(solr_endpoint, params=params)
    response.encoding = "utf-8"
    result = response.text
    #logging.info(result)
    return convert_solr_response(result)


def convert_solr_response_file(input_path, output_path):
    with open(input_path, 'rb') as input_file:
        input_string = input_file.read().replace('\n', '')
        output_json = convert_solr_response(input_string)
        output_string = jsonbson.dumps_json(output_json, True)
        with open(output_path, 'w') as output_file:
            output_file.write(output_string)


def convert_solr_response(input_string):
    result = {}
    xmletree_root = ET.fromstring(input_string)
    if xmletree_root is not None:
        root_lst_list = xmletree_root.findall("lst")
        if root_lst_list is not None:
            for root_lst in root_lst_list:
                if root_lst.get("name") == "antaMetrics":
                    anta_lst_list = root_lst.findall("lst")
                    if anta_lst_list is not None:
                        for anta_lst in anta_lst_list:
                            if anta_lst.get("name") == "tfidf":
                                result["tfidf"] = convert_solr_response_tfidf(anta_lst)
                            elif anta_lst.get("name") == "gl":
                                result["gl"] = convert_solr_response_gl(anta_lst)

    return result


def convert_solr_response_tfidf(tfdif_root):
    result_tfidf = {}
    # str
    tfidf_str_list = tfdif_root.findall("str")
    if tfidf_str_list is not None:
        for tfidf_str in tfidf_str_list:
            result_tfidf[tfidf_str.get("name")] = tfidf_str.text
    # int
    tfidf_int_list = tfdif_root.findall("int")
    if tfidf_int_list is not None:
        for tfidf_int in tfidf_int_list:
            result_tfidf[tfidf_int.get("name")] = tfidf_int.text
    # long
    tfidf_long_list = tfdif_root.findall("long")
    if tfidf_long_list is not None:
        for tfidf_long in tfidf_long_list:
            result_tfidf[tfidf_long.get("name")] = tfidf_long.text
    # lst
    tfidf_lst_list = tfdif_root.findall("lst")
    if tfidf_lst_list is not None:
        for tfidf_lst in tfidf_lst_list:
            if tfidf_lst.get("name") == "documents":
                # lst
                documents_lst_list = tfidf_lst.findall("lst")
                if documents_lst_list is not None:
                    result_documents = []
                    for documents_lst in documents_lst_list:
                        result_document = {}
                        result_document["document_id"] = documents_lst.get("name")
                        # str
                        document_str_list = documents_lst.findall("str")
                        if document_str_list is not None:
                            for document_str in document_str_list:
                                result_document[document_str.get("name")] = document_str.text
                        # lst
                        document_lst_list = documents_lst.findall("lst")
                        if document_lst_list is not None:
                            for document_lst in document_lst_list:
                                if document_lst.get("name") == "terms":
                                    # lst
                                    terms_lst_list = document_lst.findall("lst")
                                    if terms_lst_list is not None:
                                        document_terms = []
                                        for terms_lst in terms_lst_list:
                                            document_term = {}
                                            document_term["term"] = terms_lst.get("name")
                                            # str
                                            term_str_list = terms_lst.findall("str")
                                            if term_str_list is not None:
                                                for term_str in term_str_list:
                                                    document_term[term_str.get("name")] = term_str.text
                                            document_terms.append(document_term)
                                        result_document["terms"] = document_terms
                        result_documents.append(result_document)
                    result_tfidf["documents"] = result_documents
    return result_tfidf


def convert_solr_response_gl(gl_root):
    result_gl = {}
    for gl_element in gl_root.getchildren():
        # str, int, long
        if gl_element.tag in ["str", "int", "long"]:
            result_gl[gl_element.get("name")] = gl_element.text
        # lst
        elif gl_element.tag == "lst":
            # terms
            if gl_element.get("name") == "terms":
                terms = []
                for gl_terms in gl_element.getchildren():

                # lst
                terms_lst_list = gl_element.findall("lst")
                if terms_lst_list is not None:
                    terms = []
                    for terms_lst in terms_lst_list:
                        term = {}
                        term["term"] = terms_lst.get("name")

                        # str
                        term_str_list = terms_lst.findall("str")
                        if term_str_list is not None:
                            for term_str in term_str_list:
                                term[term_str.get("name")] = term_str.text
                        # int
                        term_int_list = terms_lst.findall("int")
                        if term_int_list is not None:
                            for term_int in term_int_list:
                                term[term_int.get("name")] = term_int.text
                        # long
                        term_long_list = terms_lst.findall("long")
                        if term_long_list is not None:
                            for term_long in term_long_list:
                                term[term_long.get("name")] = term_long.text
                        terms.append(term)
                    result_gl["terms"] = terms
    return result_gl


def test_direct():
    #result = extract_solr_metrics("tfidf")
    result = extract_solr_metrics("gl")
    logging.info(jsonbson.dumps_json(result, True))


def test_file():
    input_path = os.path.join(os.getcwd(),"graph","data","gl-enb.xml")
    output_path = os.path.join(os.getcwd(),"graph","data","gl-enb.json")
    convert_solr_response_file(input_path, output_path)


test_direct()
#test_file()
