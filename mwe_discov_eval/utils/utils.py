# -*- coding: utf-8 -*-
"""
Created on Thu Jan  7 15:09:20 2021

@author: nicol
"""
import json
import codecs

def save_json(dictionary, path):
    with codecs.open(path, 'w', 'utf8') as fileout:
        json.dump(dictionary, fileout, indent=1)


def load_json(path):
    with codecs.open(path, 'r', 'utf8') as filein:
        dictionary = json.load(filein)
    return dictionary
