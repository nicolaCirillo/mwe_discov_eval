# -*- coding: utf-8 -*-
"""
Created on Tue Jan 12 08:47:09 2021

@author: nicol
"""
import codecs
import os
import re

from mwe_discov_eval import messages


def remove_heading(filename):
    with codecs.open(filename, 'r', 'utf8') as filein:
        with codecs.open(filename+'_', 'w', 'utf8') as fileout:
            total = int(next(filein).split(' ')[0])
            for line in messages.pbar(filein, total=total):
                fileout.write(line)
    os.remove(filename)
    os.rename(filename+'_', filename)


def add_heading(filename):
    with codecs.open(filename, 'r', 'utf8') as filein:
        dim = len(next(filein).split(" ")) - 1
        words = 1
        for line in messages.pbar(filein):
            words += 1
    with codecs.open(filename+'_', 'w', 'utf8') as fileout:
        fileout.write(str(words) + ' ' + str(dim) + '\n')
        with codecs.open(filename, 'r', 'utf8') as filein:
            for line in messages.pbar(filein, total=words):
                fileout.write(line)
    os.remove(filename)
    os.rename(filename+'_', filename)


def join_ngrams(filename, n, sep='_'):
    with codecs.open(filename, 'r', 'utf8') as filein:
        with codecs.open(filename+'_', 'w', 'utf8') as fileout:
            for line in messages.pbar(filein):
                line = line.split(' ')
                entry, vector = line[:n], line[n:]
                entry = sep.join(entry)
                line = ' '.join([entry] + vector)
                fileout.write(line)
    os.remove(filename)
    os.rename(filename+'_', filename)


def sub_separator(filename, old_sep, new_sep):
    with codecs.open(filename, 'r', 'utf8') as filein:
        heading = next(filein)
        total, dim = heading.split(' ')
        total, dim = int(total), int(dim)
        with codecs.open(filename+'_', 'w', 'utf8') as fileout:
            fileout.write(heading)
            for line in messages.pbar(filein, total=total):
                line = line.split(' ')
                entry, vector = ' '.join(line[:-dim]), line[-dim:]
                entry = re.sub('{}(?=[A-Z])'.format(old_sep), new_sep, entry)
                line = ' '.join([entry] + vector)
                fileout.write(line)
    os.remove(filename)
    os.rename(filename+'_', filename)


def n_words(filename):
    with codecs.open(filename, 'r', 'utf8') as filein:
        heading = next(filein)
        return int(heading.split(' ')[0])
