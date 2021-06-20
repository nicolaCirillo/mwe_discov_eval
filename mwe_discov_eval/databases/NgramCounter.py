# -*- coding: utf-8 -*-
"""
Created on Tue Dec 15 07:54:48 2020

@author: nicol
"""
from collections import Counter
from bounter import bounter
from nltk import ngrams as ng


from mwe_discov_eval import messages
from .SqlDatabase import SqlDatabase
from mwe_discov_eval.utils import utils

FEATS_ID = {'surface': 0, 'lemma': 1, 'pos': 2}
DEFAULT_FEATS = ['surface', 'lemma_pos']


class NgramCounter(SqlDatabase):
    """Counts n-gram occurrences in a corpus.

     Parameters
     ----------
     fileroot: str
         Path to the file.
     feats: list, default=['surface', 'lemma_pos']
         List of feats to associate with each n-gram. Valid feats are
         'surface', 'lemma', 'pos' or each combination of these joined with an
         '_'. It is also possible to join to a feature the keyword 'sorted'
         with an '_' to obtain a feature where word order is ignored.
     sep: str, default='\t'
         Separator character used to separate compound feats  in the output
         (e.g. 'dog_NOUN').

    Examples
    ----------
    >>> sentences = ConllIterator('sample.conllu')
    >>> sentences.set_itermode('sent', keys=["lemma", "pos"])
    >>> counter = NgramCounter('sample_counter')
    >>> counter.count_ngrams(sentences, [1,2,3,4,5])

    >>> counter = NgramCounter.load('sample_counter')
    >>> counter.aggregate_by('lemma_pos')

    """

    def __init__(self, fileroot: str, feats=DEFAULT_FEATS, sep='\t', new=True):
        NgramCounter._feats_integrity(feats)
        self.feats = list(set(feats))
        self.sep = sep
        db_file = fileroot + '.db'
        self.info_file = fileroot + '.info.json'
        info = {'feats': self.feats, 'sep': self.sep}
        utils.save_json(info, self.info_file)
        super().__init__(db_file, new=new)
        messages.new_display()

    @staticmethod
    def _feats_integrity(feats):
        ALLOWED = ['sorted', 'surface', 'lemma', 'pos']
        for feat in feats:
            for f in feat.split('_'):
                if f not in ALLOWED:
                    raise ValueError("'{}' is not a valid feat.".format(f))

    @staticmethod
    def _gen_ngrams(sent, n):
        ngrams = [' '.join(ngram) for ngram in ng(sent, n)]
        return ngrams

    @classmethod
    def load(cls, fileroot):
        """Loads a NgramCounter from file.

        """
        info_file = fileroot + '.info.json'
        params = utils.load_json(info_file)
        return cls(fileroot, **params, new=False)

    def _get_feat(self, word, feat, sep):
        word = word.split(sep)
        sort = False
        feat = feat.split('_')
        if 'sorted' in feat:
            sort = True
            feat.remove('sorted')
        out = [word[FEATS_ID[f]] for f in feat]
        if sort:
            out = sorted(out)
        out = self.sep.join(out)
        return out

    def _counts_to_db(self, counter, sep, commit_each=10000):
        messages.msg("Saving counter to sql database...")
        self.connect()
        rows = list()
        for i, (ngram, freq) in messages.pbar(enumerate(counter.items())):
            ngram = ngram.split(' ')
            n = len(ngram)
            row = [n, freq]
            for feat in self.feats:
                v = ' '.join([self._get_feat(w, feat, sep) for w in ngram])
                row.append(v)
            rows.append(row)
        self.insert_data(messages.pbar(rows), 'ngram_counts')

    def count_ngrams(self, sentences, n, use_bounter=True, sep='\t',
                     **bounterargs):
        """Counts n-gram occurrences in a corpus.

        Counts n-gram occurrences in a corpus and inserts the output in an
        SQLite database.

        Parameters
        ----------
        sentences: Iterable
            Iterable of sentences. Each sentence must be a list of strings
            representing word features separated with the character that
            is passed to the 'sep' argument of this function.
        n: int or list of int
            length of the n-grams
        use_bounter: bool, default=True
            If True, the counts are performed via bounter, a probabilistic and
            memory efficient counter. If false, they are performed via regular
            Counter. The use of bounter is strongly recommended when working
            with a large corpus.
        sep: str, default '\t'
            The character that separates the features of each word in the
            input.
        **bounterargs
            keyword arguments passed to the bounter constructor if used.

        """
        messages.msg("Counting ngrams of length {}...".format(n))
        if use_bounter:
            bounterargs.setdefault('size_mb', 1024)
            counter = bounter(**bounterargs)
        else:
            counter = Counter()
        for sent in sentences:
            if type(n) == list:
                ngrams = list()
                for i in n:
                    ngrams += NgramCounter._gen_ngrams(sent, i)
            else:
                ngrams = NgramCounter._gen_ngrams(sent, n)
            counter.update(ngrams)
        messages.done()
        self._counts_to_db(counter, sep)

    def _init_db(self, db_file):
        fields = [('length', 'int', 'NOT NULL'), ('freq', 'int', 'NOT NULL')]
        fields += [(f, 'text', 'NOT NULL') for f in self.feats]
        self.new_table('ngram_counts', fields)

    def aggregate_by(self, key: str):
        """Sum frequency counts over a given field.

        Parameters
        ----------
        key: str
            the field over which to compute the sum.

        """
        messages.msg("Aggregating values...")
        new_tb = '{key}_counts'.format(key=key)
        self.connect()
        super().aggregate_by(new_tb, 'ngram_counts', ['freq'],
                             other_f=['length'], grp_f=[key])
        self.disconnect()
        messages.done()
