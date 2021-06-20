# -*- coding: utf-8 -*-
"""
Created on Sat Jan  9 11:18:59 2021

@author: nicol
"""
from collections import defaultdict
import numpy as np

from mwe_discov_eval import messages
from .utils import MeasureCalculator, DatabaseManager


class SynEntCalc(MeasureCalculator):

    def __init__(self):
        super().__init__(default_dict=defaultdict(lambda: [0, 0]))

    @staticmethod
    def _entropy(rowid, ent, N):

        if N == 1:
            entropy = 0
        else:
            entropy = (-1 / np.log(N)) * ent
        return rowid, entropy

    def _upd_dict(self, v_base: str, v_freq: int):
        b_freq, rowid = self.base_frqs[(v_base, )]
        p = v_freq/b_freq
        ent = p * np.log(p)
        super()._upd_dict(rowid, ent, 1)

    def _dict_to_list(self):
        return super()._dict_to_list(add_n=False, fun=self._entropy)

    def compute(self, ngram_db, table: str, field: str, v_table: str):
        in_fld = [field, 'freq']
        db_manager = DatabaseManager(ngram_db, v_table, in_fld)
        new_fld = [('syn_entropy', 'float')]
        db_manager.new_fields(new_fld, table=table)
        for n in messages.pbar(range(2, db_manager.n_max+1)):
            messages.computing_measure(new_fld[0][0], n)
            self.base_frqs = ngram_db.to_dict(table, [field],
                                              ['freq', 'rowid'], n)
            variant_freqs, _ = db_manager.get_iterator(n)
            for i, (v_base, v_freq) in enumerate(variant_freqs):
                self._upd(v_base, v_freq)
            db_manager.save(self._get_list())
        db_manager.finalize(self)
