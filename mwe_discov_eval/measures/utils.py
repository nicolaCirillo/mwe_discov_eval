# -*- coding: utf-8 -*-
"""
Created on Tue Apr  6 09:01:46 2021

@author: nicol
"""
import os
from shutil import copyfile
from copy import deepcopy


from mwe_discov_eval import messages
from mwe_discov_eval.databases import NgramDb

new_display = True


def set_display(new_disp: bool):
    global new_display
    new_display = new_disp


class DatabaseManager():

    @staticmethod
    def copy_db(ngram_db):
        messages.msg('Copying database...')
        outroot = ngram_db.fileroot + '_'
        if not os.path.exists(outroot+'.info.json'):
            copyfile(ngram_db.db, outroot+'.db')
            copyfile(ngram_db.info_file, outroot+'.info.json')
        messages.done()
        return NgramDb.load(outroot)

    @staticmethod
    def del_and_rename(ngram_db, output_db):
        os.remove(ngram_db.db)
        os.remove(ngram_db.info_file)
        os.rename(output_db.db, ngram_db.db)
        os.rename(output_db.info_file, ngram_db.info_file)

    def __init__(self, ngram_db, in_tb: str, in_fld: 'list',
                 save_every=-1):
        if new_display:
            messages.new_display()
        self.sv_every = save_every
        self.ngram_db = ngram_db
        self.output_db = self.copy_db(ngram_db)
        self.ngram_db.connect(), self.output_db.connect()
        self.n_max = ngram_db.n_max
        self.ngram_db.set_query(in_tb, in_fld)
        self.in_table = in_tb
        self.new = False

    def _aggregate_tmp(self, sum_fld: list, n_keys=1):
        messages.msg('Aggregating  values...')
        for f in sum_fld:
            self.new_flds.remove(f)
        grp_f, other_f = self.new_flds[:n_keys], self.new_flds[n_keys:]
        self.output_db.aggregate_by(self.new_tb[4:], self.new_tb, sum_fld,
                                    other_f, grp_f)
        self.output_db.drop_table(self.new_tb)
        self.new_tb = self.new_tb[4:]

    def finalize(self, calculator, sum_fld=None):
        self.ngram_db.disconnect()
        if self.new:
            if self.sv_every > 0 and calculator.has_dict:
                self._aggregate_tmp(sum_fld, calculator.n_keys)
            self.output_db.upd_info(self.new_tb)
        self.output_db.disconnect()
        self.del_and_rename(self.ngram_db, self.output_db)
        self.ngram_db._load_info()
        messages.done()

    def new_table(self, new_table: str, new_fields: list):
        if self.sv_every > 0:
            new_table = 'tmp_' + new_table
        self.output_db.new_table(new_table, new_fields)
        self.new_tb = new_table
        self.new_flds = list(list(zip(*new_fields))[0])
        self.new = True

    def new_fields(self, new_fields: list, table=None):
        if table is None:
            table = self.in_table
        self.output_db.new_fields(table, new_fields)
        self.new_tb = table
        self.new_flds = list(list(zip(*new_fields))[0])

    def to_list(self, table: str, fields: list, n: int):
        return self.output_db.to_list(table, fields, n)

    def to_dict(self, table: str, key_fields: list, value_fields: list,
                n: int):
        return self.output_db.to_dict(table, key_fields, value_fields, n)

    def get_iterator(self, n: int, pbar=True):
        it, N = self.ngram_db[n]
        if pbar:
            it = messages.pbar(it, total=N)
        return it, N

    def save(self, output_list: list):
        if self.new:
            self.output_db.insert_data(output_list, self.new_tb)
        else:
            self.output_db.update_data(output_list, self.new_tb,
                                       self.new_flds)

    def save_every(self, output_list: list, i: int):
        if self.sv_every > 0 and (i+1) % self.sv_every == 0:
            self.save(output_list)


class MeasureCalculator():

    def __init__(self, default_dict=None, n_keys=None):
        if default_dict is not None:
            self.default_dict = default_dict
            self._default_dict_gen = deepcopy(default_dict)
            self.has_dict = True
        else:
            self.default_list = list()
            self.has_dict = False
        self.n_keys = n_keys

    def _upd_list(self, *args):
        self.default_list.append(tuple(args))

    def _upd_dict(self, key, *values):
        for i, v in enumerate(values):
            self.default_dict[key][i] += v

    def _upd(self, *args):
        if self.has_dict:
            return self._upd_dict(args[0], *args[1:])
        else:
            return self._upd_list(*args)

    def _dict_to_list(self, add_n=False, fun=lambda *x: tuple(x)):
        out_list = list()
        if add_n:
            n = [self.n]
        else:
            n = list()
        if self.n_keys:
            for key, values in self.default_dict.items():
                out_list.append(fun(*key, *n, *values))
        else:
            for key, values in self.default_dict.items():
                out_list.append(fun(key, *n, *values))
        self.default_dict = deepcopy(self._default_dict_gen)
        return out_list

    def _get_list(self, **dict_kwargs):
        if self.has_dict:
            out_list = self._dict_to_list(**dict_kwargs)
        else:
            out_list = self.default_list
        for item in messages.pbar(out_list):
            yield item
        self.default_list = list()
