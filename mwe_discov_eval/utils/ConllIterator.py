# -*- coding: utf-8 -*-
"""
Created on Tue Nov 17 08:01:55 2020

@author: nicol
"""
import codecs
from os import path
from . import utils


from mwe_discov_eval import messages


DEFAULT_IDX_DICT = {"id": 0, "surface": 1, "lemma": 2, "pos": 3}
DEFAULT_LOWER = ['lemma']


class ConllIterator:
    """Iterator for conll files.

    A simple iterator that yields words or sentences contained in a conll file.
    The behaviour of this iterator are controlled via 'set_itermode' method.
    This iterator also generates a json file with the number of word and
    sentences contained in the corpus.

    Parameters
    ----------
    conll_file : str
        Path to the conll file.
    idx_dict: dic, default = {'surface': 1, 'lemma': 2, 'pos': 3}
        Associate each index of the tab-separated lines of the conll file
        with a keyword representing that feature.
    lower: list, default=['lemma']
        The list of elements that must be lowercased in the output of the
        iterator.
    codec: str, default='utf8'
        The codec used to open the conll file.

    Examples
    ----------
    >>> corpus = 'sample.conllu'
    >>> idx_dict = {"id": 0, "surface": 1, "lemma": 2, "pos": 3}
    >>> sentences = ConllIterator(corpus, idx_dict, codec='utf8')
    >>> sentences.set_itermode('sent', keys=["lemma", "pos"])
    >>> for s in sentences:
    ...     print(s)
    ['il\tDET', 'modifica\tNOUN', [...], '»\tPUNCT']
    ['otto\tNUM', 'mese\tNOUN', [...], '.\tPUNCT']
    [...]
    ['entrambi\tPRON', 'accomunare\tVERB', [...], '.\tPUNCT']

    """

    def __init__(self, conll_file: str, idx_dict=DEFAULT_IDX_DICT,
                 lower=['lemma'], codec='utf8'):
        self.idx_dict = idx_dict
        self.lower = lower
        self.filename = conll_file
        self.codec = codec
        self._itermode = 'sent'
        self._itermode_kwargs = {'keys': 'all', 'join_values': '\t'}
        info_file = self.filename.split('.')
        info_file[-1] = 'info.json'
        self.info_file = '.'.join(info_file)
        if path.exists(self.info_file):
            self._load_info()
        else:
            self.sentences = None
            self.tokens = None
            self._save_info()

    def set_itermode(self, mode: {'word', 'sent'}, keys='all',
                     join_values='\t', ignore_compound=False):
        """Controls the behaviour of the iterator.

        If mode is set to 'word', the iterator yields the words, if it is set
        'sent', then the iterator yelds the sentences.

        Parameters
        ----------
        mode : {'word', 'sent'}
           Controls whether to iterate over words or sentences
        keys: list, optional
            The list of the features of a word that are retuned (default is
            'all' which implies that all the elements passed to the 'idx_dict'
            argument of the constructor are returned)
        join_values: str or False, default='\t'
            If a string is passed, each word will be represented as a string in
            which the feature of the word are joined with the value passed to
            this argument. If False is passed, each word will be represented as
            a list of his features.
        ignore_compound: bool, default= False
            If True, skips words that have been separated (in which the 'id'
            feature contains a '-'). Make sure to pass the 'id' key to the
            'idx_dict'  argument of the constructor if you want to set this
            parameter to True.

        """
        assert (not ignore_compound) or 'id' in self.idx_dict
        self._itermode = mode
        self._itermode_kwargs = {'keys': keys, 'join_values': join_values,
                                 'ignore_compound': ignore_compound}

    def _is_compound(self, line):
        if line == '':
            return False
        else:
            values = line.split('\t')
            id_ = self._get_value('id', values)
            if '-' in id_:
                return True
            else:
                return False

    def _get_value(self, key, values):
        idx = self.idx_dict[key]
        try:
            if key in self.lower:
                return values[idx].lower()
            else:
                return values[idx]
        except IndexError:
            return "_"

    def _parse_line(self, line, keys='all', join_values='\t'):
        line = line.strip()
        if keys == 'all':
            keys = list(self.idx_dict.keys())
        if line == '':
            return None
        values = line.split('\t')
        out_values = [self._get_value(key, values) for key in keys]
        if join_values:
            out_values = join_values.join(out_values)
        return out_values

    def _save_info(self):
        info = {'tokens': self.tokens, 'sentences': self.sentences}
        utils.save_json(info, self.info_file)

    def _load_info(self):
        info = utils.load_json(self.info_file)
        self.sentences, self.tokens = info['sentences'], info['tokens']

    def _iter_words(self, keys='all', join_values='\t', ignore_compound=False):
        corpus = codecs.open(self.filename, 'r', self.codec)
        for i, line in enumerate(corpus):
            if line.startswith('#'):
                continue
            if ignore_compound:
                if self._is_compound(line):
                    continue
            yield self._parse_line(line, keys, join_values)
        corpus.close()

    def _iter_sent(self, keys='all', join_values='\t', ignore_compound=False):
        sent = list()
        for line in self._iter_words(keys, join_values, ignore_compound):
            if line:
                sent.append(line)
            elif sent:
                yield sent
                sent = list()

    def __iter__(self):
        mode, kwargs = self._itermode, self._itermode_kwargs
        if mode == 'sent':
            if self.sentences is None:
                for i, item in messages.pbar(
                        enumerate(self._iter_sent(**kwargs))):
                    yield item
                self.sentences = i+1
                self._save_info()
            else:
                for item in messages.pbar(self._iter_sent(**kwargs),
                                          total=self.sentences):
                    yield item
        elif mode == 'word':
            if self.tokens is None:
                for i, item in messages.pbar(
                        enumerate(self._iter_words(**kwargs))):
                    yield item
                self.tokens = i+1
                self._save_info()
            else:
                for item in messages.pbar(self._iter_words(**kwargs),
                                          total=self.tokens):
                    yield item
        else:
            raise ValueError(mode)

    def sample(self, n=100):
        """Prints the first n elements yielded by this iterator.

        Parameters
        ----------
        n: int, default=100
            The number of elements to print.

        """
        for i, item in enumerate(self):
            print(item)
            if i >= n:
                break

    def save_as_text(self, filename: str, keys='all', join_values='\t',
                     ignore_compound=False):
        """Save the sentences yielded by this iterator in a text file.

        Parameters
        ----------
        filename: str
        keys: dict, optional
            The list of the features of a word that are retuned (default is
            'all' which implies that all the elements passed to the 'idx_dict'
            argument of the constructor are returned)
        join_values: str, default='\t'
            Each word will be represented as a string in which the feature of
            the word are joined with the value passed to this argument.

        """
        assert join_values
        with codecs.open(filename, 'w', 'utf8') as fileout:
            sentences = self._iter_sent(keys, join_values, ignore_compound)
            for sent in messages.pbar(sentences, total=self.sentences):
                fileout.write(' '.join(sent) + '\n')
