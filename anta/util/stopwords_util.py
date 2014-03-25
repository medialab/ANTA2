#!/usr/bin/python
# -*- coding: utf-8 -*-
# coding=utf-8

import os

def merge_stopwords(input_path):
    with open(input_path, 'rb') as input_file:
        result_dict = {}
        for line in input_file:
            result_dict[line.strip()] = ""
        for key in sorted(result_dict.keys()):
            print key


def int_stopwords():
    for x in range(0,1000):
        print x

#merge_stopwords(os.path.join(os.getcwd(),"conf","stop_words_en.txt"))
#int_stopwords()
