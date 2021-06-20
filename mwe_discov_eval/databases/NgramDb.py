# -*- coding: utf-8 -*-
"""
Created on Fri Jan  1 20:58:38 2021

@author: nicol
"""
from shutil import copyfile

from .SqlDatabase import SqlDatabase
from . import sql_commands as sql
from mwe_discov_eval import messages
from mwe_discov_eval.utils import utils


COMMIT_EACH = 1000


class NgramDb(SqlDatabase):
    """An interface to an SQL database that stores n-gram statistics.

    An interface to an SQLite database containing n-gram statistics. This class
    is passed as input to the functions that compute n-gram measures to compute
    that measure and store the output in the SQL database. Use
    'from_NgramCounter' to create a new instance and 'load' to load an NgramDb
    from file.

     Parameters
     ----------
     fileroot: str
         Path to the file.

    Examples
    ----------
    To create a new NgramDb:

    >>> from mwe_discov_eval.databases import NgramCounter, NgramDb
    >>> counter = NgramCounter.load('sample_counter')
    >>> ngram_db = NgramDb.from_NgramCounter(counter, 'sample_ngram_db')

    To compute an n-gram measure:

    >>> from mwe_discov_eval.databases import NgramCounter
    >>> from mwe_discov_eval.measures.am_extended import compute_measure, pmi
    >>> ngram_db = NgramDb.load('sample_ngram_db')
    >>> compute_measure(ngam_db, pmi)

    """
    def __init__(self, fileroot: str):
        self.fileroot = fileroot
        db_file = fileroot + '.db'
        self.info_file = fileroot + '.info.json'
        super().__init__(db_file, new=False)
        self._query = None

    @classmethod
    def from_NgramCounter(cls, ngram_counter, fileroot: str):
        """Creates a new NgramDb from an NgramCounter.

        Parameters
        ----------
        ngram_counter: NgramCounter
            The NgramCounter from which to create the NgramDb.
        fileroot: str
            Path to the file of the new NgramCounter.

        Returns
        -------
        NgramDb

        """
        messages.new_display()
        copyfile(ngram_counter.db, fileroot + '.db')
        new_cls = cls(fileroot)
        new_cls.connect()
        new_cls._gen_info()
        new_cls.disconnect()
        new_cls._load_info()
        return new_cls

    @classmethod
    def load(cls, fileroot: str):
        """Loads an NgramDb from file

        Parameters
        ----------
        fileroot: str
            Path to the file.

        Returns
        -------
        NgramDb

        """
        new_cls = cls(fileroot)
        new_cls._load_info()
        return new_cls

    def _load_info(self):
        info = utils.load_json(self.info_file)
        self.n_max = info['n_max']
        self.num_rows = info['num_rows']

    def _gen_info(self):
        messages.msg("Generating info file...")
        num_rows = dict()
        tables = self.get_tables()
        if tables:
            n_max = self.query(sql.MAX_LEN.format(table=tables[0]))[0][0]
            for t in tables:
                for n in range(1, n_max+1):
                    n_rows = self.query(sql.ROW_COUNT.format(table=t), n)
                    num_rows.setdefault(t, dict())[n] = n_rows[0][0]
        else:
            n_max, num_rows = 0, None
        info = {'n_max': n_max, 'num_rows': num_rows}
        utils.save_json(info, self.info_file)
        messages.done()

    def upd_info(self, table: str):
        """Updates the .info.json file with regards to a table.

        Parameters
        ----------
        table: str
            The name of the table of which to update the info

        """
        for n in range(1, self.n_max+1):
            n_rows = self.query(sql.ROW_COUNT.format(table=table), n)
            self.num_rows.setdefault(table, dict())[n] = n_rows[0][0]
        info = {'n_max': self.n_max, 'num_rows': self.num_rows}
        utils.save_json(info, self.info_file)
        messages.done()

    def _load_ngrams(self, n):
        query = sql.format_query(self._query_table, self.query_fields,
                                 cond=True, limit=self._limit,
                                 offset=self._offset)
        self.execute(query, n)

    def __getitem__(self, n):
        self._load_ngrams(n)
        num = self.num_rows[self._query_table][str(n)]
        return self.cur, num

    def set_query(self, table: str, fields=['*'], limit=None, offset=None):
        self._limit = limit
        self._offset = offset
        self._query_table = table
        self.query_fields = fields

    def frequency_threshold(self, table, threshold):
        messages.msg("Filtering ngrams...")
        delete = sql.DELETE.format(table=table)
        self.execute(delete, threshold)
        self.commit()
        messages.done()
        self.upd_info(table)
