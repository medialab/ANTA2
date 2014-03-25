#!/usr/bin/python
# -*- coding: utf-8 -*-

import pymongo
from bson.objectid import ObjectId
from bson.errors import InvalidId

from anta.util import config
from anta.util import exceptions
from anta.util import jsonbson

# database
ANTADB = "anta"

# Collections
HISTORY = "history"
NE = "ne"
TEXT = "text"
USER = "user"

config = config.config["mongodb"]


try:
    mongodb = pymongo.MongoClient(config['host'], config['port'])
except pymongo.errors.ConnectionFailure as e:
    print "ERROR: connexion failure to MongoDB : {}".format(e)


################
# ANTA MAIN DB #
################
def get_user_corpora(username):
    if username:
        return mongodb[ANTADB][USER].find({'username': username})


##########
# CORPUS #
##########
def list_corpora():
    return [x for x in mongodb.database_names() if x.startswith("anta_")]


def corpus_database_name(corpus):
    return "".join(["anta_",corpus])


def create_corpus(corpus):
    mongodb[corpus_database_name(corpus)]


def delete_corpus(corpus):
    mongodb.drop_database(corpus_database_name(corpus))


def empty_corpus(corpus):
    mongodb[corpus_database_name(corpus)][HISTORY].drop()
    mongodb[corpus_database_name(corpus)][NE].drop()
    mongodb[corpus_database_name(corpus)][TEXT].drop()
    mongodb[corpus_database_name(corpus)][USER].drop()


def init_corpus_indexes(corpus):
    index_id = ('_id', pymongo.ASCENDING)

    mongodb[corpus][HISTORY].ensure_index([index_id], safe=True)
    mongodb[corpus][NE].ensure_index([index_id], safe=True)
    mongodb[corpus][TEXT].ensure_index([index_id], safe=True)
    mongodb[corpus][USER].ensure_index([index_id], safe=True)


