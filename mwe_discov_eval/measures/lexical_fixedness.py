# -*- coding: utf-8 -*-
"""
Created on Sun Jan 24 11:07:17 2021

@author: nicol
"""
import numpy as np
from dataclasses import dataclass, field
from gensim.similarities.index import AnnoyIndexer
import pickle

from mwe_discov_eval import messages
from .utils import initialize, finalize, save_every


def create_sim_dict(file, vectors, min_sim=0.55, topn=10, num_trees=200):
    indexer = AnnoyIndexer(vectors, num_trees=num_trees)
    sim_dict = dict()
    for w in messages.pbar(vectors.vocab):
        sim = indexer.most_similar(vectors.get_vector(w), topn)
        sim_dict[w] = [s for s in sim if s[1] > min_sim]
    with open(file, 'wb') as fileout:
        pickle.dump(sim_dict, fileout)


@dataclass
class LexEntCalculator():
    variant_dict: dict
    lex_entropy: list = field(default_factory=list)

    def set_var_frqs(self, var_frqs: dict):
        self.var_frqs = var_frqs

    def sim(self, ngram):
        words = ngram.split(" ")
        variants = list()
        for i, w in enumerate(words):
            sim = self.variant_dict[w]
            new_v = [' '.join(words[:i] + [w[0]] + words[i+1:]) for w in sim]
            variants += new_v
        return variants

    def upd_stats(self, rowid, ngram):
        entropy = 0
        V = self.sim(ngram)
        if V:
            V_freqs = [self.var_frqs.get((v, )) for v in V]
            sum_frqs = np.sum(V_freqs)
            for f in V_freqs:
                p = f / sum_frqs
                entropy += p * np.log(p)
            entropy = (-1 / np.log(len(V))) * entropy
        self.lex_entropy.append((rowid, entropy))

    def get_list(self):
        return self.lex_entropy


def entropy(ngram_db, variant_dict: dict, table: str, field: str):
    in_fld = ['rowid', field]
    output_db, n_max = initialize(ngram_db, table, in_fld)
    new_fld = [('lex_entropy', 'float')]
    output_db.new_fields(table, new_fld)
    calculator = LexEntCalculator(variant_dict)
    for n in range(2, n_max+1):
        messages.computing_measure(new_fld[0][0], n)
        var_frqs = ngram_db.to_dict(table, [field], ['freq'], n)
        calculator.set_var_frqs(var_frqs)
        base_freqs, N = ngram_db[n]
        for i, (rowid, ngram) in enumerate(messages.pbar(base_freqs, total=N)):
            calculator.upd_stats(rowid, ngram)
            save_every(output_db, calculator.get_list(), table, new_fld[0][0],
                       i)
    output_db.update_data(calculator.get_list(), table, new_fld[0][0])
    finalize(ngram_db, output_db)


def lex_fixedness(ngram_db, variant_dict: dict, table: str, field: str,
                  m_field: str):
    in_fld = ['rowid', field, m_field]
    output_db, n_max = initialize(ngram_db, table, in_fld)
    new_fld = [('lex_fixedness_' + m_field, 'float')]
    output_db.new_fields(table, new_fld)
    for n in range(2, n_max+1):
        messages.msg("Collecting statistics from {}grams...".format(n))
        lemma_pmis = ngram_db.to_dict('lemma_counts', ['lemma'], ['pmi'], n)
        ngram_freqs = ngram_db[n]
        lex_fixedness = list()
        for i, (ngram, pmi, rowid) in enumerate(messages.pbar(ngram_freqs,
                                                       total=num_rows[str(n)])):
            V = sim(ngram, neighbours_dict)
            V_pmis = list()
            for v in V:
                try:
                    V_pmis.append(lemma_pmis[v])
                except KeyError:
                    pass
            if len(V_pmis) > 1:
                fixedness = (pmi - np.mean(V_pmis)) / np.var(V_pmis)
            else:
                fixedness = 0
            lex_fixedness.append((rowid, fixedness))
            if (i+1) % save_every == 0:
                output_db.update_data(lex_fixedness, in_table, 'lex_fixedness')
                lex_fixedness = list()
    output_db.update_data(lex_fixedness, in_table, 'lex_fixedness')
    ngram_db.disconnect()
    output_db.disconnect()
    del_and_rename(ngram_db, output_db)
    messages.done()
