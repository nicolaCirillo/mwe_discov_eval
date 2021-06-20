# -*- coding: utf-8 -*-
"""
Created on Tue Jan 19 10:09:05 2021

@author: nicol
"""
import numpy as np

from mwe_discov_eval import messages
from .utils import DatabaseManager, MeasureCalculator


def c_sum(vecs):
    return np.sum(vecs, 0)


def c_mult(vecs):
    return np.prod(vecs, 0)


def mult_sum(vecs):
    return c_sum(vecs + [c_mult(vecs)])


class CompCalc(MeasureCalculator):
    def __init__(self, fun=c_sum):
        self.fun = fun
        super().__init__()

    @staticmethod
    def cosine(self, v1, v2):
        cos_sim = np.dot(v1, v2) / (np.linalg.norm(v1) * np.linalg.norm(v2))
        return cos_sim

    def _upd_list(self, rowid, ngram: str):
        words = ngram.split(' ')
        ngram_vec = self.ngram_vecs[(ngram, )]
        sw_vecs = [self.sw_vecs[(w, )] for w in words]
        comp_vec = self.fun(sw_vecs)
        c = self.cosine(ngram_vec, comp_vec)
        super()._upd_list(rowid, c)

    def compute(self, ngram_db, embeddings_db, table: str, field: str,
                save_every=-1):
        in_fld = ['rowid', field]
        db_manager = DatabaseManager(ngram_db, table, in_fld,
                                     save_every=save_every)
        embeddings_db.connect()
        new_fld = [('comp_' + self.fun.__name__, 'float')]
        db_manager.new_fields(new_fld)
        self.sw_vecs = embeddings_db.to_dict(1)
        for n in messages.pbar(range(2, db_manager.n_max+1)):
            messages.computing_measure(new_fld[0][0], n)
            ngrams, _ = db_manager.get_iterator(n)
            self.ngram_vecs = embeddings_db.to_dict(n)
            for i, (rowid, ngram) in enumerate(ngrams):
                self._upd(rowid, ngram)
                db_manager.save_every(self._get_list(), i)
            db_manager.save(self._get_list())
        embeddings_db.disconnect()
        db_manager.finalize(self)
