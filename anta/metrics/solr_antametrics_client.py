#!/usr/bin/python
# -*- coding: utf-8 -*-
# coding=utf-8

import logging
import xml.etree.ElementTree as ET

import requests

from anta.util import console
from anta.util import config
from anta.util import jsonbson

console.setup_console()
config_solr = config.config["solr"]
solr_endpoint = config_solr["endpoint"] + "antametrics"

# curl "http://localhost:8983/solr/emaps/antametrics?q=*:*&wt=xml&rows=0&metrics=tfidf"
# curl "http://localhost:8983/solr/emaps/antametrics?q=*:*&wt=xml&rows=0&metrics=gl"

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
    tfidf_str_list = tfdif_root.findall("str")
    if tfidf_str_list is not None:
        for tfidf_str in tfidf_str_list:
            result_tfidf[tfidf_str.get("name")] = tfidf_str.text
    tfidf_lst_list = tfdif_root.findall("lst")
    if tfidf_lst_list is not None:
        for tfidf_lst in tfidf_lst_list:
            if tfidf_lst.get("name") == "documents":
                documents_lst_list = tfidf_lst.findall("lst")
                if documents_lst_list is not None:
                    result_documents = []
                    for documents_lst in documents_lst_list:
                        result_document = {}
                        result_document["document_id"] = documents_lst.get("name")
                        document_str_list = documents_lst.findall("str")
                        if document_str_list is not None:
                            for document_str in document_str_list:
                                result_document[document_str.get("name")] = document_str.text
                        document_lst_list = documents_lst.findall("lst")
                        if document_lst_list is not None:
                            for document_lst in document_lst_list:
                                if document_lst.get("name") == "terms":
                                    terms_lst_list = document_lst.findall("lst")
                                    if terms_lst_list is not None:
                                        document_terms = []
                                        for terms_lst in terms_lst_list:
                                            document_term = {}
                                            document_term["term"] = terms_lst.get("name")
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
    gl_str_list = gl_root.findall("str")
    if gl_str_list is not None:
        for gl_str in gl_str_list:
            result_gl[gl_str.get("name")] = gl_str.text
    gl_lst_list = gl_root.findall("lst")
    if gl_lst_list is not None:
        for gl_lst in gl_lst_list:
            if gl_lst.get("name") == "terms":
                terms_lst_list = gl_lst.findall("lst")
                if terms_lst_list is not None:
                    terms = []
                    for terms_lst in terms_lst_list:
                        term = {}
                        term["term"] = terms_lst.get("name")
                        term_str_list = terms_lst.findall("str")
                        if term_str_list is not None:
                            for term_str in term_str_list:
                                term[term_str.get("name")] = term_str.text
                        terms.append(term)
                    result_gl["terms"] = terms
    return result_gl


def test():
    #result = extract_solr_metrics("tfidf")
    result = extract_solr_metrics("gl")
    logging.info(jsonbson.dumps_json(result, True))


test()