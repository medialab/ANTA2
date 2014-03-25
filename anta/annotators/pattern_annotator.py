#!/usr/bin/python
# -*- coding: utf-8 -*-
# coding=utf-8

import os

def parse_stopwords(language):
    stopwords = []
    if language == "fr":
        stopwords_path = os.path.join(os.getcwd(),"conf","stop_words_fr.txt")
    elif language == "en":
        stopwords_path = os.path.join(os.getcwd(),"conf","stop_words_en.txt")
    with open(stopwords_path, 'rb') as stopwords_file:
        for stopword in stopwords_file:
            stopwords.append(stopword.strip())
    return stopwords


stopwords_fr = parse_stopwords("fr")
stopwords_en = parse_stopwords("en")


def text_to_pattern_tree(text, language):
    if language == "fr":
        from pattern.fr import parsetree
    elif language == "en":
        from pattern.en import parsetree
    tree = parsetree(text, tokenize=True, tags=True, chunks=True, relations=True, lemmata=True, light=False)
    #print tree
    return tree


def extract_text_pos_tags(text, language, pos_tags):
    result = []
    if language == "fr":
        stopwords = stopwords_fr
    elif language == "en":
        stopwords = stopwords_en
    tree = text_to_pattern_tree(text, language)
    for sentence in tree:
        for chunk in sentence.chunks:
            if chunk.type in pos_tags:
                for lema in chunk.lemmata:
                    if lema and not lema in stopwords:
                        result.append(lema)
    return " ".join(result)


def parse_text_as_json(text, language):
    return pattern_tree_to_json(text_to_pattern_tree(text, language))


def pattern_tree_to_json(tree):
    mdocument = {}
    msections = []
    for sentence in tree:
        msections.append(pattern_sentence_to_json(sentence))
    mdocument["sections"] = msections
    return mdocument


def pattern_sentence_to_json(sentence):
    msentence = {"id": sentence.id, "language": sentence.language}
    mchunks = []
    if sentence.chunks:
        for chunk in sentence.chunks:
            mchunks.append(pattern_chunk_to_json(chunk))
    msentence["chunks"] = mchunks
    return msentence


def pattern_chunk_to_json(chunk):
    # chunk.string, chunk.lemmata, chunk.type, chunk.role, chunk.relations, chunk.pnp, chunk.conjunctions, chunk.anchor_id)
    mchunk = {"string": chunk.string, "lemmata": chunk.lemmata, "pos": chunk.type, "role": chunk.role}
    if chunk.relations:
        relations = []
        for relation in relations:
            relations.append({"ref": relation[0], "role": relation[0]})
        mchunk["relations"] = relations
    mwords = []
    if chunk.words:
        for word in chunk.words:
            mwords.append(pattern_word_to_json(word))
    mchunk["words"] = mwords
    return mchunk


def pattern_word_to_json(word):
    mword = {"index": word.index, "string": word.string, "lemma": word.lemma, "pos": word.type}
    if word.pnp:
        mword["pnp"] = word.pnp
    # tags [WORD, POS, CHUNK, PNP, RELATION, ANCHOR, LEMMA]
    # tags:[u'today', u'NN', u'I-NP', 'I-PNP', 'NP-OBJ-4*NP-SBJ-5', u'today']
    tags = word.tags
    mword["chunk"] = tags[2]
    mword["pnp"] = tags[3]
    mword["relation"] = tags[4]
    return mword

