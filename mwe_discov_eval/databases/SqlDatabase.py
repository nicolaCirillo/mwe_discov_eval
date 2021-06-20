# -*- coding: utf-8 -*-
"""
Created on Sat Dec 19 07:25:17 2020

@author: nicol
"""
import os
import codecs
import re
import sqlite3
from sqlite3 import Error

from mwe_discov_eval import messages
from . import sql_commands as sql

COMMIT_EACH = 1000


class SqlDatabase:
    """Abstract interface to SQLlite databases

    Parameters
    ----------
    db_file: str
        Path to the database file.
    new: bool, optional.
        If true, creates a new database at the specified path, loads a
        pre-existent database otherwise. Default is 'True'.

    """

    def __init__(self, db_file: str, new=True, **kwargs):
        self.db = db_file
        self.conn = None
        self.cur = None
        if new:
            if os.path.exists(db_file):
                os.remove(db_file)
            self.connect()
            self._init_db(db_file, **kwargs)
            self.disconnect()

#    @abc.abstractmethod
    def _init_db(self, db_file):
        self.connect()
        self.disconnect()

    def connect(self):
        """Creates a connection to the database.

        """
        try:
            self.conn = sqlite3.connect(self.db)
            self.cur = self.conn.cursor()
        except Error as e:
            print(e)
            raise e

    def disconnect(self):
        """Close the connection with the database.

        """
        self.cur = None
        self.conn.close()
        self.conn = None

    def commit(self):
        """Commit changes into the database.

        """
        self.conn.commit()

    def execute(self, sql: str, *args):
        """Executes an SQL command.

        Parameters
        ----------
        sql: str
            the sql command.
        *args:
            items used to replace the '?' placeholders in the sql command
            if any.

        Notes
        -----
        This function does not return any result. To query the database use the
        'query' function instead.

        """
        try:
            self.cur.execute(sql, args)
        except Error as e:
            print(e)
            self.disconnect()
            raise e

    def query(self, query: str, *args):
        """Executes a query and return the result.

        Parameters
        ----------
        query: str
            the sql command.
        *args:
            items used to replace the '?' placeholders in the sql command
            if any.

        Returns
        -------
        list
            the result of the query.
        """
        try:
            self.cur.execute(query, args)
        except Error as e:
            print(e)
        return self.cur.fetchall()

    def get_tables(self):
        """Returns the names of the tables in the database.

        Returns
        -------
        list
            The names of the tables.

        """
        tables = [t[0] for t in self.query(sql.TABLES)]
        return tables

    def get_fields(self, table: str):
        """Returns the names of the fields of a table.

        Parameters
        ----------
        table: str
            the name of the table.

        Returns
        -------
        list
            The names of the fields.

        """
        fields = [f[1] for f in self.query(sql.FIELDS.format(table=table))]
        return fields

    def drop_table(self, table):
        """Drops a table.

        Parameters
        ----------
        table: str
            the name of the table.

        """
        drop = sql.DROP.format(table=table)
        self.execute(drop)
        self.commit()

    def drop_columns(self, table: str, columns: list):
        """Drops columns from a table.

        Parameters
        ----------
        table: str
            the name of the table.

        columns: list
            the names of the columns that must be dropped.

        Notes
        -----
        This function creates a copy of the table without the given columns and
        then drops the old table so it is very time-consuming.

        """
        FIELDS = "PRAGMA table_info({table}_old)"
        RENAME = "ALTER TABLE {table} RENAME TO {table}_old"
        COPY = '''INSERT INTO {table}
                    SELECT {fields}
                    FROM {table}_old;'''
        rename = RENAME.format(table=table)
        self.execute(rename)
        fields = [(f[1], f[2]) for f in
                  self.query(FIELDS.format(table=table))]
        fields = [f for f in fields if f[0] not in columns]
        messages.msg("Copying database...")
        self.new_table(table, fields)
        fields = ','.join(list(zip(*fields))[0])
        self.execute(COPY.format(table=table, fields=fields))
        self.commit()
        self.drop_table('{table}_old'.format(table=table))
        messages.done()

    def new_table(self, table: str, fields: list):
        """Creates a new table.

        Parameters
        ----------
        table: str
            the name of the table.
        fields: list of tuples
            a list of tuples representing the fields, the strings in the tuples
            are joined with a space in the SQL command.

        """
        fields = ','.join([' '.join(f) for f in fields])
        create = sql.CREATE.format(table=table, field=fields)
        if table in self.get_tables():
            self.drop_table(table)
        self.execute(create)
        self.commit()

    def new_fields(self, table: str, fields: list):
        """Adds new fields to a table.

        Parameters
        ----------
        table: str
            the name of the table.
        fields: list of tuples
            a list of tuples representing the fields, the strings in the tuples
            are joined with a space in the SQL command.

        """
        old_fields = self.get_fields(table)
        fields = [f for f in fields if f[0] not in old_fields]
        if fields:
            for f in fields:
                alter = sql.ALTER.format(table=table, field=' '.join(f))
                self.execute(alter)
            self.commit()

    def insert_data(self, values: list, table: str):
        """Inserts new records in the table.

        Parameters
        ----------
        values: list
            a list with the values to insert in each record. The values are
            inserted in the given order.
        table: str
            the name of the table.

        """
        messages.msg('Saving...')
        fields = self.get_fields(table)
        qmarks = ','.join(['?']*len(fields))
        fields = ','.join([f for f in fields])
        insert = sql.INSERT.format(table=table, fields=fields, values=qmarks)
        for i, v in enumerate(values):
            self.execute(insert, *v)
            if (i+1) % COMMIT_EACH == 0:
                self.commit()
        self.commit()
        messages.done()

    def update_data(self, rowids_values: list, table: str, fields: list):
        """Updates values of given cells.

        Parameters
        ----------
        rowids_values: list of lists
            a list with the rowids of the records to update and the values to
            be insert. e.g.
            [(rowid1, value1, value2), (rowid2, value3, value4)]
        table: str
            the name of the table.
        fields: list
            the name of the fields to update.

        """
        messages.msg('Saving...')
        if type(fields) == list:
            fields = ', '.join([f + '=?' for f in fields])
        else:
            fields += ' = ?'
        update = sql.UPDATE.format(table=table, fields=fields,)
        for i, values in enumerate(rowids_values):
            values = list(values[1:]) + [values[0]]
            self.execute(update, *values)
            if (i+1) % COMMIT_EACH == 0:
                self.commit()
        self.commit()
        messages.done()

    def aggregate_by(self, new_tb: str, old_tb: str, sum_f: list,
                     other_f=list(), grp_f=list()):
        """Creates a new table with a SUM field.

        Parameters
        ----------
        new_tb: str
            the name of the new table.
        old_tb: str
            the name of the table from which to take the data.
        sum_f: list
            the name of the field over which to compute the sum.
        other_f: list, opltional
            the names of other fields copied in the new table. Default is [].
        grp_f: list, optional
            the name of the fields with which to create groups, Default is [].


        """
        if grp_f:
            AGGREGATE = sql.AGGREGATE + ''' GROUP BY {keys}'''.format(
                    keys=','.join(grp_f))
        fields = ','.join(other_f + grp_f)
        sum_ = ', '.join([sql.SUM.format(field=f) for f in sum_f])
        aggregate = AGGREGATE.format(new_t=new_tb, old_t=old_tb,
                                     fields=fields, sum_=sum_)
        if new_tb in self.get_tables():
            self.drop_table(new_tb)
        self.execute(aggregate)
        self.commit()

    def to_dict(self, table: str, key_fields: list, value_fields: list,
                n: int):
        """Returns the records in the database as a dict.

        Returns the records in the database as a dict for a specific n-gram
        length.

        Parameters
        ----------
        table: str
            The name of the table from which to take the records
        key_fields: list
            The fields to use as key
        value_fields: list
            The fields to use as values
        n: int
            The length of the n-grams.

        Returns
        -------
        dict
            The dict with the recods, The format is:
            {(key1, key2): [value1, value2]}

        """
        n_key = len(key_fields)
        fields = key_fields + value_fields
        query = sql.format_query(table, fields, cond=True)
        results = self.query(query, n)
        dic = {tuple(r[:n_key]): r[n_key:] for r in results}
        return dic

    def to_list(self, table: str, fields: list, n: int):
        """Returns the records in the database as a list.

        Returns the records in the database as a list for a specific n-gram
        length.

        Parameters
        ----------
        table: str
            The name of the table from which to take the records
        fields: list
            The fields to use as values
        n: int
            The length of the n-grams.

        Returns
        -------
        list
            The list with the recods.

        """
        query = sql.format_query(table, fields, cond=True)
        results = self.query(query, n)
        return results

    def to_text(self, file: str, table: str, field: str, n: int, sub=None,
                mode='w'):
        """Writes the records in the database in a text file.

        Writes the records in the database in a text file for a specific n-gram
        length.

        Parameters
        ----------
        file: str
            The path of the file
        table: str
            The name of the table from which to take the records
        fields: list
            The name of the fields taken.
        n: int
            The length of the n-grams.
        sub: tuple, optional
            A regexp pattern and a replacement to perform 're.sub' on every
            line of the resulting file. Default is None, which means that no
            substitution is performed.

        """
        query = sql.format_query(table, [field], cond=True)
        self.execute(query, n)
        total = self.num_rows[table][str(n)]
        with codecs.open(file, mode, 'utf8') as fileout:
            for value in messages.pbar(self.cur, total=total):
                if sub:
                    line = re.sub(sub[0], sub[1], value[0]) + '\n'
                else:
                    line = value[0] + '\n'
                fileout.write(line)
