# -*- coding: utf-8 -*-
"""
Created on Sun Dec 13 07:55:11 2020

@author: nicol
"""
import codecs
import re
import messages

expected_items = None
tags_to_simplify = None
lemmatization_dict = None
surface_position = None
lemma_position = None
pos_position = None
_idx_to_del = None

def set_lemmatization_params(dic, surf_posit, lemma_posit, pos_posit):
    global lemmatization_dict
    global surface_position
    global lemma_position
    global pos_position
    lemmatization_dict = dic
    surface_position = surf_posit
    lemma_position = lemma_posit
    pos_position = pos_posit


def set_expected_items(value: int):
    global expected_items
    expected_items = value


def set_tags_to_simplify(tags):
    global tags_to_simplify
    tag = '(?<={})'
    regex = '('
    regex += '|'.join([tag.format(t) for t in tags]) + ')'
    tags_to_simplify = regex + ':.*?(?=\t)'


def replace_missing(line):
    if expected_items is None:
        raise ValueError(expected_items)
    items = line.split('\t')
    if len(items) < expected_items:
        items += ['_']*(expected_items-len(items))
    return '\t'.join(items)


def replace_spaces(line):
    line = re.sub(r'((?![\t\n])\s)+', '_', line)
    return line


def simplify_tags(line):
    line = re.sub(tags_to_simplify, '', line)
    return line


def modify_lemmatization(line):
    if '\t' in line:
        items = line[:-1].split("\t")
        surf = items[surface_position].lower()
        pos = items[pos_position]
        try:
            new_lemma = lemmatization_dict[(surf, pos)]
            items[lemma_position] = new_lemma
            line = "\t".join(items) + "\n"
        except KeyError:
            pass
    return line


def delete_separated(line):
    global _idx_to_del
    regex1 = re.compile(r"^(\d+)-(\d+)\t")
    regex2 = "^{}|{}\t"
    search = regex1.search(line)
    if search:
        _idx_to_del = re.compile(regex2.format(search.group(1),
                                               search.group(2)))
        return line
    elif _idx_to_del:
        if _idx_to_del.search(line):
            return None
        else:
            return line
    else:
        return line


def convert_wac(filein, fileout, codec='utf8', line_funcs=[]):
    """Remove xml tags and extra newlines.
    The resulting file will have sentences separated with a newline.
    Args:
        filein (str): the path to the input file
        fileout (str): the path to the output file
        codec (str, optional): the codecs used to decode the file.
            Default 'utf8'
    """
    XML_TAG = re.compile(r'<[^<]+>')

    filein = codecs.open(filein, 'r', codec)
    fileout = codecs.open(fileout, 'w', codec)
    messages.msg("Converting file...")
    try:
        last_is_newline = True
        for line in messages.pbar(filein):
            line = XML_TAG.sub('', line)
            if line == '\n':
                if last_is_newline:
                    pass
                else:
                    fileout.write(line)
                    last_is_newline = True
            else:
                for func in line_funcs:
                    line = func(line)
                if line:
                    fileout.write(line)
                last_is_newline = False
    except Exception as e:
        filein.close()
        fileout.close()
        raise e
    filein.close()
    fileout.close()
    messages.done()
