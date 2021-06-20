# -*- coding: utf-8 -*-
"""
Created on Tue Dec 22 10:46:58 2020

@author: nicol
"""
from collections import defaultdict
import numpy as np

from mwe_discov_eval import messages
from .utils import DatabaseManager, MeasureCalculator, set_display


def _skipos(skipgram):
    skipgram = skipgram.split(' ')
    i = skipgram.index('*')
    left_c = [w.split('\t')[1] for w in skipgram[:i]]
    right_c = [w.split('\t')[1] for w in skipgram[i+1:]]
    return ' '.join(left_c + ['*'] + right_c)


def _reduce_context(skipgram):
    reduced_c = list()
    words = skipgram.split(' ')
    if words[0] != '*':
        reduced_c.append(' '.join(words[1:]))
    if words[-1] != '*':
        reduced_c.append(' '.join(words[:-1]))
    return reduced_c


def _skipgram(ngram: str, i: int):
    ngram = ngram.split(' ')
    return ' '.join(ngram[:i] + ["*"] + ngram[i+1:])


class SkipgramCalc(MeasureCalculator):

    def __init__(self):
        super().__init__(default_dict=defaultdict(lambda: [0]))

    def _upd_dict(self, ngram: str, freq: int):
        words = ngram.split(' ')
        for j, _ in enumerate(words):
            super()._upd_dict(_skipgram(ngram, j), freq)

    @staticmethod
    def _gen_skipos(skipgram, *args):
        out = (skipgram, _skipos(skipgram), *args)
        return out

    def compute(self, ngram_db, table: str, field: str, save_every=-1):
        in_fld = [field, 'freq']
        db_manager = DatabaseManager(ngram_db, table, in_fld,
                                     save_every=save_every)
        new_table = 'skipgram_counts'
        new_fields = [('skipgram', 'text'), ('skipos', 'text'),
                      ('length', 'int'), ('freq', 'int')]
        db_manager.new_table(new_table, new_fields)
        for n in messages.pbar(range(2, db_manager.n_max+1)):
            messages.computing_measure('skipgram freq', n)
            ngram_freqs, N = db_manager.get_iterator(n)
            self.n = n
            for i, (ngram, freq) in enumerate(ngram_freqs):
                self._upd(ngram, freq)
                db_manager.save_every(self._get_list(add_n=True,
                                                     fun=self._gen_skipos), i)
            db_manager.save(self._get_list(add_n=True, fun=self._gen_skipos))
        db_manager.finalize(self)


class LexPredCalc(MeasureCalculator):

    def _upd_list(self, ngram: str, freq: int):
        words = ngram.split(' ')
        for j, w in enumerate(words):
            skip = _skipgram(ngram, j)
            prob = (freq / self.N) / (self.skipgram_freqs[(skip,)][0] / self.N)
            super()._upd_list(w, skip, self.n, prob)

    def compute(self, ngram_db, table: str, field: str, save_every=-1):
        in_fld = [field, 'freq']
        db_manager = DatabaseManager(ngram_db, table, in_fld,
                                     save_every=save_every)
        new_table = 'lex_context_counts'
        new_fields = [('word', 'text'), ('skipgram', 'text'),
                      ('length', 'int'), ('pred', 'float')]
        db_manager.new_table(new_table, new_fields)
        for n in messages.pbar(range(2, db_manager.n_max+1)):
            messages.computing_measure('lexical predictability', n)
            ngram_freqs, self.N = db_manager.get_iterator(n)
            self.n = n
            self.skipgram_freqs = db_manager.to_dict('skipgram_counts',
                                                     ['skipgram'], ['freq'], n)
            for i, (ngram, freq) in enumerate(ngram_freqs):
                self._upd(ngram, freq)
                db_manager.save_every(self._get_list(), i)
            db_manager.save(self._get_list())
        db_manager.finalize(self)


def gen_skipos(ngram_db, display=True):
    if display:
        messages.new_display()
    messages.msg('Generating skipos statistics...')
    ngram_db.connect()
    ngram_db.aggregate_by('skipos_counts', 'skipgram_counts',
                          'freq', ['length'], ['skipos'])
    ngram_db.upd_info('skipos_counts')
    ngram_db.disconnect()
    messages.done()


class SynContextCalc(MeasureCalculator):

    def __init__(self):
        super().__init__(default_dict=defaultdict(lambda: [0]), n_keys=2)

    def _upd_dict(self, ngram: str, freq: int):
        words = ngram.split(' ')
        for j, w in enumerate(words):
            skip = _skipos(_skipgram(ngram, j))
            super()._upd_dict((w, skip), freq)

    def compute(self, ngram_db, table: str, field: str, save_every=-1):
        in_fld = [field, 'freq']
        db_manager = DatabaseManager(ngram_db, table, in_fld,
                                     save_every=save_every)
        new_table = 'syn_context_counts'
        new_fields = [('word', 'text'), ('skipos', 'text'), ('length', 'int'),
                      ('freq', 'int')]
        db_manager.new_table(new_table, new_fields)
        for n in messages.pbar(range(2, db_manager.n_max+1)):
            messages.computing_measure('syntactic context freq', n)
            ngram_freqs, _ = db_manager.get_iterator(n)
            self.n = n
            for i, (ngram, freq) in enumerate(ngram_freqs):
                self._upd(ngram, freq)
                db_manager.save_every(self._get_list(add_n=True), i)
            db_manager.save(self._get_list(add_n=True))
        db_manager.finalize(self)


class SynPredCalc(MeasureCalculator):

    def _upd_list(self, rowid: int, skipos: str, freq: int):
        prob = (freq / self.N) / (self.skipos_freqs[(skipos,)][0] / self.N)
        super()._upd_list(rowid, prob)

    def compute(self, ngram_db, save_every=-1):
        in_table = 'syn_context_counts'
        in_fld = ['rowid', 'skipos', 'freq']
        db_manager = DatabaseManager(ngram_db, in_table, in_fld,
                                     save_every=save_every)
        new_fields = [('pred', 'float')]
        db_manager.new_fields(new_fields)
        for n in messages.pbar(range(2, db_manager.n_max+1)):
            messages.computing_measure('syntactic predictability', n)
            self.skipos_freqs = db_manager.to_dict('skipos_counts', ['skipos'],
                                                   ['freq'], n)
            syn_c_freqs, self.N = db_manager.get_iterator(n)
            for i, (rowid, skipos, freq) in enumerate(syn_c_freqs):
                self._upd(rowid, skipos, freq)
                db_manager.save_every(self._get_list(), i)
            db_manager.save(self._get_list())
        db_manager.finalize(self)


class WordLprCalc(MeasureCalculator):

    def _upd_list(self, rowid: int, word: str, skipgram: str, prob: float):
        lpr = prob / self.syn_prob[(word, _skipos(skipgram))][0]
        super()._upd_list(rowid, lpr)

    def compute(self, ngram_db, save_every=-1):
        in_table = 'lex_context_counts'
        in_fld = ['rowid', 'word', 'skipgram', 'pred']
        db_manager = DatabaseManager(ngram_db, in_table, in_fld,
                                     save_every=save_every)
        new_fields = [('lpr', 'float')]
        db_manager.new_fields(new_fields)
        for n in messages.pbar(range(2, db_manager.n_max+1)):
            messages.computing_measure('word lpr', n)
            self.syn_prob = db_manager.to_dict(
                    'syn_context_counts', ['word', 'skipos'], ['pred'], n)
            lex_prob, _ = db_manager.get_iterator(n)
            for i, (rowid, word, skipgram, prob) in enumerate(lex_prob):
                self._upd(rowid, word, skipgram, prob)
                db_manager.save_every(self._get_list(), i)
            db_manager.save(self._get_list())
        db_manager.finalize(self)


class MaxPredCalc(MeasureCalculator):

    def _upd_list(self, rowid: int, w: str, skipgram: str, value: float):
        pre_values = [
                self.pre.get((w, c), [0])[0] for c in
                _reduce_context(skipgram)]
        max_value = max([value] + pre_values)
        super()._upd_list(rowid, max_value)

    def compute(self, ngram_db, table: str, field: str, save_every=-1):
        in_fld = ['rowid', 'word', 'skipgram', field]
        db_manager = DatabaseManager(ngram_db, table, in_fld,
                                     save_every=save_every)
        new_fields = [('max_' + field, 'float')]
        db_manager.new_fields(new_fields)
        for n in messages.pbar(range(2, db_manager.n_max+1)):
            messages.computing_measure(new_fields[0][0], n)
            self.pre = db_manager.to_dict(table, ['word', 'skipgram'],
                                          ['max_' + field], n-1)
            current, _ = db_manager.get_iterator(n)
            for i, (rowid, w, skipgram, value) in enumerate(current):
                self._upd(rowid, w, skipgram, value)
                db_manager.save_every(self._get_list(), i)
            db_manager.save(self._get_list())
        db_manager.finalize(self)


class NgramLprCalc(MeasureCalculator):
    FUN = {'sum': np.sum, 'prod': np.prod, 'min': min}
    measure = 'lpr'

    def __init__(self, agg_fun='prod'):
        self.agg_fun = agg_fun
        super().__init__()

    def _upd_list(self, rowid: int):
        lpr = self.FUN[self.agg_fun]([v[0] for v in self.w_lpr.fetchmany(
                self.n)])
        super()._upd_list(rowid, lpr)

    def compute(self, ngram_db, table: str, save_every=-1):
        in_table = 'lex_context_counts'
        in_fld = ['max_' + self.measure]
        db_manager = DatabaseManager(ngram_db, in_table, in_fld,
                                     save_every=save_every)
        new_fields = [(self.agg_fun + '_' + self.measure, 'float')]
        db_manager.new_fields(new_fields, table=table)
        for n in messages.pbar(range(2, db_manager.n_max+1)):
            messages.computing_measure(new_fields[0][0], n)
            ngrams_id = db_manager.to_list(table, ['rowid'], n)
            self.w_lpr, N = db_manager.get_iterator(n, pbar=False)
            self.n = n
            ngrams_id = messages.pbar(ngrams_id)
            for i, rowid in enumerate(ngrams_id):
                self._upd(rowid[0])
                db_manager.save_every(self._get_list(), i)
            db_manager.save(self._get_list())
        db_manager.finalize(self)


class NgramPredCalc(NgramLprCalc):
    measure = 'pred'

    def _upd_list(self, rowid: int):
        pred = self.FUN[self.agg_fun](
                [np.log(v[0])for v in self.w_lpr.fetchmany(self.n)])
        MeasureCalculator()._upd_list(rowid, pred)


class LPRCalc():

    def __init__(self, agg_fun='prod'):
        self.agg_fun = agg_fun

    def compute(self, ngram_db, table: str, field: str, save_every=-1):
        set_display(False)
        pbar = messages.manual_pbar(total=8)
        messages.new_display()
        calc = SkipgramCalc()
        calc.compute(ngram_db, table, field, save_every)
        pbar.update()
        calc = LexPredCalc()
        calc.compute(ngram_db, table, field, save_every)
        pbar.update()
        gen_skipos(ngram_db, False)
        pbar.update()
        calc = SynContextCalc()
        calc.compute(ngram_db, table, field, save_every)
        pbar.update()
        calc = SynPredCalc()
        calc.compute(ngram_db, save_every)
        pbar.update()
        calc = WordLprCalc()
        calc.compute(ngram_db, save_every)
        pbar.update()
        calc = MaxPredCalc()
        calc.compute(ngram_db, 'lex_context_counts', 'lpr', save_every)
        pbar.update()
        calc = NgramLprCalc(self.agg_fun)
        calc.compute(ngram_db, table)
        pbar.update()
        messages.close_manual_pbar(pbar)
        set_display(True)


class PredCalc():

    def __init__(self, agg_fun='sum'):
        self.agg_fun = agg_fun

    def compute(self, ngram_db, table: str, field: str, save_every=-1):
        set_display(False)
        pbar = messages.manual_pbar(total=4)
        messages.new_display()
        calc = SkipgramCalc()
        calc.compute(ngram_db, table, field, save_every)
        pbar.update()
        calc = LexPredCalc()
        calc.compute(ngram_db, table, field, save_every)
        pbar.update()
        calc = MaxPredCalc()
        calc.compute(ngram_db, 'lex_context_counts', 'pred', save_every)
        pbar.update()
        calc = NgramPredCalc(self.agg_fun)
        calc.compute(ngram_db, table)
        pbar.update()
        messages.close_manual_pbar(pbar)
        set_display(True)
