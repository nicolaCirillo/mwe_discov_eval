# -*- coding: utf-8 -*-
"""
Created on Tue Dec 29 14:57:47 2020

@author: nicol
"""
from collections import defaultdict

from mwe_discov_eval import messages
from .utils import DatabaseManager, MeasureCalculator


class FairDispPointCalc(MeasureCalculator):

    def __init__(self, measure):
        def dict_len():
            return [0]*len(self.measure)
        if type(measure) != list:
            measure = [measure]
        self.measure = measure
        super().__init__(default_dict=defaultdict(dict_len))

    @staticmethod
    def _divide(ngram, i):
        words = ngram.split(' ')
        return ' '.join(words[:i]), ' '.join(words[i:])

    def _avg(self, *values):
        return [v / (self.n-1) for v in values]

    def _dict_to_list(self):
        return super()._dict_to_list(fun=self._avg)

    def _get_w_freq(self, w: str, dic: int):
        if dic == 1:
            f = self.freq_1.get((w,), [1])[0]
        elif dic == 2:
            f = self.freq_2.get((w,), [1])[0]
        else:
            raise Exception()
        return f

    def _compute_odd(self, ngram, freq, i):
        values = list()
        w_1, w_2 = self._divide(ngram, i)
        f_1, f_2 = self._get_w_freq(w_1, 1), self._get_w_freq(w_2, 2)
        for m in self.measure:
            values.append(m(f_1, f_2, freq, self.N))
        w_1, w_2 = self._divide(ngram, self.n - i)
        f_1, f_2 = self._get_w_freq(w_2, 1), self._get_w_freq(w_1, 2)
        for v, m in zip(values, self.measure):
            v += m(f_1, f_2, freq, self.N)
        return values

    def _compute_even(self, ngram, freq, i):
        values = list()
        w_1, w_2 = self._divide(ngram, i)
        f_1, f_2 = self._get_w_freq(w_1, 1), self._get_w_freq(w_2, 1)
        for m in self.measure:
            values.append(m(f_1, f_2, freq, self.N))
        return values

    def _get_compute(self, i):
        if self.n / i == 2:
            return self._compute_even
        else:
            return self._compute_odd

    def _upd_dict(self, rowid, ngram, freq, i):
        compute = self._get_compute(i)
        values = compute(ngram, freq, i)
        super()._upd_dict(rowid, *values)

    def _set_freq_dicts(self, db_manager, i, to_dict_args):
        if self.n / i == 2:
            self.freq_1 = db_manager.to_dict(*to_dict_args, i)
            self.freq_2 = self.freq_1
        else:
            self.freq_1 = db_manager.to_dict(*to_dict_args, i)
            self.freq_2 = db_manager.to_dict(*to_dict_args, self.n - i)

    def compute(self, ngram_db, table: str, field: str):
        in_fld = ['rowid', field, 'freq']
        db_manager = DatabaseManager(ngram_db, table, in_fld)
        new_fld = [('fdp_' + m.__name__, 'float') for m in self.measure]
        db_manager.new_fields(new_fld)
        for n in messages.pbar(range(2, db_manager.n_max+1)):
            fld_str = ', '.join([f[0] for f in new_fld])
            messages.computing_measure(fld_str, n)
            self.n = n
            for i in range(1, n//2+1):
                freq_12, N = db_manager.get_iterator(n)
                self.N = N
                to_dict_args = [table, [field], ['freq']]
                self._set_freq_dicts(db_manager, i, to_dict_args)
                for rowid, ngram, freq in freq_12:
                    self._upd(rowid, ngram, freq, i)
            out_list = self._get_list()
            db_manager.save(out_list)
        db_manager.finalize(self)
