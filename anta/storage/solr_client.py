#!/usr/bin/python
# -*- coding: utf-8 -*-
# coding=utf-8

import sunburnt

from anta.util import config

# solr $ANTA_HOME/solr-conf

config_solr = config.config["solr"]


class SOLRInterface():
    si = sunburnt.SolrInterface(config_solr['endpoint'])

    def initialize(self):
        """Initialize schema"""
        self.si.init_schema()


    def delete_all(self):
        """Delete all documents"""
        self.si.delete_all()
        self.si.commit()


    def add_documents(self, documents):
        """Add some documents"""
        self.si.add(documents)
        self.si.commit()


    def add_document(self, document):
        """Add a document"""
        self.si.add(document)


    def commit(self):
        self.si.commit()


    def delete_document(self, document):
        """Delete a document"""
        pass

