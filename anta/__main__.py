#!/usr/bin/python
# -*- coding: utf-8 -*-
# coding=utf-8

import argparse
import logging

from anta.util import console
from anta.util import config
from anta.storage.solr_client import SOLRInterface
from anta.readers import enb_reader2
from anta.readers import ipcc_reader
from anta.readers import nyt_reader
from anta.readers import unfccc_reader

# usage:
# python anta clean -c corpus
# python anta conf -c corpus -d corpus_conf_dir_name
# python anta import -c corpus

console.setup_console()


def clean_corpus(args):
    corpus = args.corpus
    logging.info("corpus: {}".format(corpus))
    # TODO clean only this corpus
    solr = SOLRInterface()
    solr.delete_all()
    solr.initialize()


def conf_corpus(args):
    corpus = args.corpus
    corpus_conf_dir_name = args.corpus_conf_dir_name
    logging.info("corpus: {}".format(corpus))
    logging.info("corpus_conf_dir_name: {}".format(corpus_conf_dir_name))
    # TODO save corpus conf 


def import_documents(args):
    corpus = args.corpus
    logging.info("corpus: {}".format(corpus))
    #enb_reader2.read()
    #ipcc_reader.read()
    #unfccc_reader.read()
    #nyt_reader.read()


# parser
parser = argparse.ArgumentParser(description='ANTA2',
                                 epilog="Actor Network Text Analyser v2")
parser.add_argument('--version',
                    action='version',
                    version='%(prog)s ')

subparsers = parser.add_subparsers(title='subcommands',
                                   description='valid subcommands',
                                   help='Choose one of theses functions')

# clean
clean_parser = subparsers.add_parser('clean',
                                     help="Be careful! Erase all documents.")
clean_parser.set_defaults(func=clean_corpus)
clean_parser.add_argument('-c',
                          '--corpus',
                          dest='corpus',
                          help='corpus identifier')

# conf
conf_parser = subparsers.add_parser('conf',
                                    help='Update corpus "conf" folder')
conf_parser.set_defaults(func=conf_corpus)
conf_parser.add_argument('-c',
                         '--corpus',
                         dest='corpus',
                         help='corpus identifier')
conf_parser.add_argument('-d',
                         '--corpus_conf_dir_name',
                         dest='corpus_conf_dir_name',
                         help='corpus directory name inside the conf/corpus')

# import
import_parser = subparsers.add_parser('import',
                                      help='Import documents')
import_parser.set_defaults(func=import_documents)
import_parser.add_argument('-c',
                           '--corpus',
                           dest='corpus',
                           help='corpus identifier')


args = parser.parse_args()
args.func(args)
