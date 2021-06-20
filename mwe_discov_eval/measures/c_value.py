# -*- coding: utf-8 -*-
"""
Created on Thu Dec 17 14:06:18 2020

@author: nicol
"""
from nltk import ngrams as ng
from collections import defaultdict
from copy import deepcopy
import numpy as np

from mwe_discov_eval import messages
from .utils import DatabaseManager, MeasureCalculator


class CValueCalc(MeasureCalculator):

    def __init__(self):
        self.prev_super_freq = defaultdict(lambda: [0, 0])
        self.curr_super_freq = defaultdict(lambda: [0, 0])
        super().__init__()

    @staticmethod
    def _gen_subsets(ngram: str):
        words = ngram.split(' ')
        n = len(words)
        for sub in ng(words, n-1):
            yield ' '.join(sub)

    def _upd_super(self, ngram: str, freq: int):
        for sub in self._gen_subsets(ngram):
            self.curr_super_freq[sub][0] += freq
            self.curr_super_freq[sub][1] += 1

    def _c_value(self, ngram: str, freq: int):
        c = np.log2(self.n)
        Ta = self.prev_super_freq.get(ngram, None)
        if Ta:
            c *= (freq - Ta[0] / Ta[1])
        else:
            c *= (freq)
        return c

    def _upd(self, rowid, ngram: str, freq: int):
        self._upd_super(ngram, freq)
        c = self._c_value(ngram, freq)
        super()._upd(rowid, c)

    def _swich_super(self):
        self.prev_super_freq = deepcopy(self.curr_super_freq)
        self.curr_super_freq = defaultdict(lambda: [0, 0])

    def compute(self, ngram_db, table: str, field: str, save_every=-1):
        in_fld = ['rowid', field, 'freq']
        db_manager = DatabaseManager(ngram_db, table, in_fld, save_every)
        new_fld = [('c_value', 'float')]
        db_manager.new_fields(new_fld)
        for n in messages.pbar(range(db_manager.n_max, 1, -1)):
            messages.computing_measure('c_value', n)
            ngram_frqs, N = db_manager.get_iterator(n)
            self.n = n
            for i, (rowid, ngram, freq) in enumerate(ngram_frqs):
                self._upd(rowid, ngram, freq)
                db_manager.save_every(self._get_list(), i)
            db_manager.save(self._get_list())
            self._swich_super()
        db_manager.finalize(self)
