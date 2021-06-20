# -*- coding: utf-8 -*-
"""
Created on Thu Apr 15 08:49:05 2021

@author: nicol
"""
import codecs

from .SqlDatabase import SqlDatabase
from mwe_discov_eval.utils import utils
from . import sql_commands as sql
from mwe_discov_eval import messages


class W2VReader():

    def __init__(self, w2v_file: str, sep=' ', dim=None):
        self.file_path = w2v_file
        self.sep = sep
        with codecs.open(self.file_path, 'r', 'utf8') as filein:
            header = filein.readline()
        self.entries, self.dim = [int(n) for n in header.split(' ')]

    def _parse_line(self, line):
        line = line.split(' ')
        if self.sep == ' ':
            n = len(line) - self.dim
            word = self.sep.join(line[:n])
        else:
            word = line[0]
            n = len(word.split(self.sep))
        embedding = [float(n) for n in line[n:]]
        return [word, n] + embedding

    def __iter__(self):
        with codecs.open(self.file_path, 'r', 'utf8') as filein:
            filein.readline()
            for line in filein:
                yield self._parse_line(line)


class EmbeddingsDb(SqlDatabase):

    def __init__(self, fileroot: str, new=True, reader=None):
        db_file = fileroot + '.db'
        self.info_file = fileroot + '.info.json'
        if reader and new:
            self.dim = reader.dim
        super().__init__(db_file, new=new, reader=reader)

    @classmethod
    def from_w2v(cls, w2v_file: str, fileroot: str):
        reader = W2VReader(w2v_file)
        new_cls = cls(fileroot, new=True, reader=reader)
        new_cls.connect()
        new_cls._gen_info(reader.dim)
        new_cls.disconnect()
        new_cls._load_info()
        return new_cls

    @classmethod
    def load(cls, fileroot: str):
        new_cls = cls(fileroot, new=False)
        new_cls._load_info()
        return new_cls

    def _gen_info(self, dim: int):
        messages.msg("Generating info file...")
        num_rows = dict()
        table = self.get_tables()[0]
        n_max = self.query(sql.MAX_LEN.format(table=table))[0][0]
        for n in range(1, n_max+1):
            n_rows = self.query(sql.ROW_COUNT.format(table=table), n)
            num_rows.setdefault(table, dict())[n] = n_rows[0][0]
        info = {'n_max': n_max, 'dim': dim, 'num_rows': num_rows}
        utils.save_json(info, self.info_file)
        messages.done()

    def _load_info(self):
        info = utils.load_json(self.info_file)
        self.dim = info['dim']
        self.n_max = info['n_max']
        self.num_rows = info['num_rows']

    def _init_db(self, db_file, reader):
        fields = [('entries', 'text'), ('length', 'int')]
        fields += [('v' + str(n), 'float') for n in range(1, self.dim+1)]
        self.new_table('embeddings', fields)
        self.insert_data(reader, 'embeddings')

    def __getitem__(self, n):
        self.connect()
        self.execute("SELECT * FROM embeddings")
        num = self.num_rows[self._query_table][str(n)]
        return self.cur, num

    def to_dict(self, n):
        table = 'embeddings'
        key_flds = ['entries']
        value_flds = ['v' + str(n) for n in range(1, self.dim+1)]
        dic = super().to_dict(table, key_flds, value_flds, n)
        return dic
