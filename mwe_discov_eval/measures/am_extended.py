# -*- coding: utf-8 -*-
"""
Created on Sun Jan 31 11:30:45 2021

@author: nicol
"""
import numpy as np

from mwe_discov_eval import messages
from .utils import DatabaseManager, MeasureCalculator


def pmi(f_sw, f_12, N):
    p_sw = [f / N for f in f_sw]
    p_12 = f_12 / N
    pmi = np.log2(p_12 / np.prod(p_sw))
    return pmi


def b_pmi(f_sw, f_12, N):
    p_sw = [f / N for f in f_sw]
    p_12 = f_12 / N
    b_pmi = np.log2((f_12 * p_12) / np.prod(p_sw))
    return b_pmi


def dice(f_sw, f_12, N=None):
    dice = len(f_sw)*f_12 / np.sum(f_sw)
    return dice


def t(f_sw, f_12, N):
    p_sw = [f / N for f in f_sw]
    p_12 = f_12 / N
    t = (p_12 - np.prod(p_sw)) / np.sqrt(p_12 / N)
    return t


class AMExtendedCalc(MeasureCalculator):

    def __init__(self, measure):
        if type(measure) != list:
            measure = [measure]
        self.measure = measure
        super().__init__()

    def _comp_measure(self, measure, ngram: str, freq: int):
        words = ngram.split(' ')
        f_sw = [self.freq_1[(w,)][0] for w in words]
        return measure(f_sw, freq, self.N)

    def _upd_list(self, rowid, ngram: str, freq: int):
        values = list()
        for m in self.measure:
            v = self._comp_measure(m, ngram, freq)
            values.append(v)
        super()._upd_list(rowid, *values)

    def compute(self, ngram_db, table: str, field: str, save_every=-1):
        in_fld = ['rowid', field, 'freq']
        db_manager = DatabaseManager(ngram_db, table, in_fld,
                                     save_every=save_every)
        new_fld = [(m.__name__, 'float') for m in self.measure]
        db_manager.new_fields(new_fld)
        self.freq_1 = db_manager.to_dict(table, [field], ['freq'], 1)
        for n in messages.pbar(range(2, db_manager.n_max+1)):
            fld_str = ', '.join([f[0] for f in new_fld])
            messages.computing_measure(fld_str, n)
            freq_12, N = db_manager.get_iterator(n)
            self.n, self.N = n, N
            for i, (rowid, ngram, freq) in enumerate(freq_12):
                self._upd(rowid, ngram, freq)
                db_manager.save_every(self._get_list(), i)
            db_manager.save(self._get_list())
        db_manager.finalize(self)
