#!/usr/bin/env python
# coding=utf-8

"""
Mysql operation wrapper.

Authors: secfree(zzd7zzd@gmail.com)
"""


import sys
import logging
import traceback
import time
import collections

import mysql.connector


class Dbmysql(object):
    """mysql-connector operation wrapper
    """
    def __init__(self, db_config):
        """

        :param db_config: dict, mysql connection configure
        :return:
        """
        self.con = None
        self.cur = None
        self.db_config = db_config

    def connect(self, retry=3):
        """
        Connect to database.

        :return: boolean, success_flag
        """
        for _ in range(retry):
            try:
                self.con = mysql.connector.connect(**self.db_config)
            except mysql.connector.Error as err:
                for frame in traceback.extract_tb(sys.exc_info()[2]):
                    filename, lineno, fn, text = frame
                    logging.error('Exception raised: File "%s", '
                                  'line %s , in %s "%s" : %s' %
                                  (filename, lineno, fn, text, str(err)))
                time.sleep(5)
                continue
            self.cur = self.con.cursor(dictionary=True)
            return True
        else:
            return False

    def reconnect(self, retry=3):
        """Ensure connection is not closed

        :param retry: int, connection retry number
        :return: boolean, success_flag
        """
        for _ in range(retry):
            try:
                self.con.reconnect()
            except mysql.connector.Error as err:
                for frame in traceback.extract_tb(sys.exc_info()[2]):
                    filename, lineno, fn, text = frame
                    logging.error('Exception raised: File "%s", '
                                  'line %s , in %s "%s" : %s' %
                                  (filename, lineno, fn, text, str(err)))
                time.sleep(5)
                continue
            self.cur = self.con.cursor(dictionary=True)
            return True
        else:
            return False

    def ensure_connect(self):
        """Ensure connection is not closed

        :return: boolean, success_flag
        """
        if self.con:
            if not self.con.is_connected() and not self.reconnect():
                return False
        elif not self.connect():
            return False
        return True

    def commit(self):
        """
        Do commit.

        return: boolean, success_flag
        """
        if not self.ensure_connect():
            return False
        try:
            self.con.commit()
        except mysql.connector.Error as err:
            for frame in traceback.extract_tb(sys.exc_info()[2]):
                filename, lineno, fn, text = frame
                logging.error('Exception raised: File "%s", '
                              'line %s , in %s "%s" : %s' %
                              (filename, lineno, fn, text, str(err)))
            return False
        return True

    def _cur_execute(self, sql, values):
        if not self.ensure_connect():
            return False
        try:
            self.cur.execute(sql, values)
        except mysql.connector.Error as err:
            for frame in traceback.extract_tb(sys.exc_info()[2]):
                filename, lineno, fn, text = frame
                logging.error('Exception raised: File "%s", '
                              'line %s , in %s "%s" : %s' %
                              (filename, lineno, fn, text, str(err)))
            return False
        return True

    def _cur_executemany(self, sql, values):
        if not self.ensure_connect():
            return False
        try:
            self.cur.executemany(sql, values)
        except mysql.connector.Error as err:
            for frame in traceback.extract_tb(sys.exc_info()[2]):
                filename, lineno, fn, text = frame
                logging.error('Exception raised: File "%s", '
                              'line %s , in %s "%s" : %s' %
                              (filename, lineno, fn, text, str(err)))
            return False
        return True

    def _cur_fetch(self):
        try:
            rows = self.cur.fetchall()
        except mysql.connector.Error as err:
            for frame in traceback.extract_tb(sys.exc_info()[2]):
                filename, lineno, fn, text = frame
                logging.error('Exception raised: File "%s", '
                              'line %s , in %s "%s" : %s' %
                              (filename, lineno, fn, text, str(err)))
            return False, None
        return True, rows

    def _execute_any(self, sql):
        if not self.ensure_connect():
            return False
        try:
            self.cur.execute(sql)
        except mysql.connector.Error as err:
            # ER_TABLE_EXISTS_ERROR, 创建已存在的表报警忽略
            if err.errno == 1050:
                return True
            for frame in traceback.extract_tb(sys.exc_info()[2]):
                filename, lineno, fn, text = frame
                logging.error('Exception raised: File "%s", '
                              'line %s , in %s "%s" : %s' %
                              (filename, lineno, fn, text, str(err)))
                return False
            return True

    def insert(self, table, row, dict_key=None):
        """Inert a row into table.

        :param table: str, table name
        :param row: dict, row values
        :dict_key: str, the column name converted from row's key
        :return: (boolean, int), (success_flag, lastrowid)
        """
        if not row:
            return True, None

        ikys = ''
        values = []

        if not dict_key:
            if not isinstance(row, dict):
                logging.warn('Wrong format row: %s' % str(row))
                return False, None

            for k in row:
                ikys += '`%s`, ' % k
                values.append(row[k])
            iqs = '%s, ' * len(row)
        else:
            if not isinstance(row, dict) or \
                    not isinstance(row.values()[0], dict):
                logging.warn('Wrong format row: %s' % str(row))
                return False, None

            ikys += '`%s`, ' % dict_key
            iqs = '%s, ' * (len(row.values()[0]) + 1)
            values.append(row.keys()[0])

            v = row.values()[0]
            for k in v:
                ikys += '`%s`, ' % k
                values.append(v[k])

        ikys = ikys[:-2]
        iqs = iqs[:-2]
        sql = 'insert into %s(%s) values(%s);' % (table, ikys, iqs)

        if not self._cur_execute(sql, values):
            return False, None
        return True, self.cur.lastrowid

    def insertmany(self, table, rows, dict_key=None, per=1000, sort=False):
        """Inert rows into a table.

        :param table: str, table name
        :param rows: list, list of row
        :param dict_key: str, the column name converted from row's key
        :param per: int, num of rows insert once
        :param sort: boolean, insert rows sort by dict_key
        :return: (boolean, int), (success_flag, lastrowid)
        """
        if not rows:
            return True, None

        ikys = ''
        values = []

        if not dict_key:
            if not isinstance(rows, list) or not isinstance(rows[0], dict):
                logging.warn("Wrong format rows, rows[0]: %s." % str(rows[0]))
                return False, None

            keys = rows[0].keys()
            iqs = '%s, ' * len(keys)

            for r in rows:
                val = []
                for k in keys:
                    if k not in r:
                        break
                    val.append(r[k])
                else:
                    values.append(val)
        else:
            if not isinstance(rows, dict) or \
                    not isinstance(rows.values()[0], dict):
                logging.warn("Wrong format rows, rows[0]: %s." % str(rows[0]))
                return False, None

            ikys += '`%s`, ' % dict_key
            keys = rows.values()[0].keys()
            iqs = '%s, ' * (len(keys) + 1)

            dks = rows.keys()
            if sort:
                dks.sort()
            for dk in dks:
                val = [dk]
                for k in keys:
                    if k not in rows[dk]:
                        break
                    val.append(rows[dk][k])
                else:
                    values.append(val)

        for k in keys:
            ikys += '`%s`, ' % k
        ikys = ikys[:-2]
        iqs = iqs[:-2]

        sql = 'insert into %s(%s) values(%s);' % (table, ikys, iqs)
        m = 0
        num = len(values)

        while m < num:
            if not self._cur_executemany(sql, values[m:m + per]):
                return False, None
            m += per
        return True, self.cur.lastrowid

    def fetch(self, table, num=None, keys=None, dict_key=None, **conditions):
        """
        Fetch rows from a table.
        :param table: str, table name.
        :param num: int, row's num limit.
        :param keys: tuple, (str_key, ...)
        :param conditions: dict, fetch conditions
        :param dict_key: str, the column's value used as row's key
        :return: (boolean, list or dict), (success_flag, fetched_rows)
        """
        if keys is None:
            keys = ()
        values = []
        sql = 'select '
        if len(keys) > 0:
            for k in keys:
                sql += '`%s`, ' % k
            sql = sql[:-2]
        else:
            sql += '*'
        sql += ' from %s ' % table
        if len(conditions) > 0:
            sql += 'where'
            for k in conditions:
                sql += ' %s=%%s and ' % k
                values.append(conditions[k])
            sql = sql[:-5]
        if num:
            sql += ' limit %s'
            values.append(num)
        sql += ';'

        if not self._cur_execute(sql, values):
            return False, None
        flag, rows = self._cur_fetch()
        if not flag:
            return False, None

        if not dict_key:
            return True, rows

        dict_rows = {}
        for r in rows:
            if dict_key not in r:
                continue
            dk = r.pop(dict_key)
            if isinstance(dk, collections.Hashable):
                dict_rows[dk] = r
        return True, dict_rows

    def update(self, table, conditions=None, **kwargs):
        """
        Update a table.
        :param table: str, table name
        :param conditions: dict, update conditions
        :param kwargs: dict, key-value pairs
        :return: boolean, success_flag
        """
        if len(kwargs) == 0:
            return True
        values = []
        sql = 'update %s set ' % table
        for k in kwargs:
            sql += '`%s` = %%s, ' % k
            values.append(kwargs[k])
        sql = sql[:-2]
        if conditions and len(conditions) > 0:
            sql += ' where'
            for k in conditions:
                sql += ' %s=%%s and ' % k
                values.append(conditions[k])
            sql = sql[:-5]
        sql += ';'

        return self._cur_execute(sql, values)

    def execute(self, sql, dict_key=None):
        """
        Execute a sql.

        :param dict_key: str, the column's value used as row's key
        :return: (boolean, *), (success_flag, result)
        """
        if not self._execute_any(sql):
            return False, None
        if not self.cur.with_rows:
            return True, None

        flag, rows = self._cur_fetch()
        if not flag:
            return False, None
        if not rows:
            return True, None
        if not dict_key:
            return True, rows

        dict_rows = {}
        for r in rows:
            if dict_key not in r:
                continue
            dk = r.pop(dict_key)
            if isinstance(dk, collections.Hashable):
                dict_rows[dk] = r
        return True, dict_rows

    def delete(self, table, **conditions):
        """delete rows

        :return: boolean, success_flag
        """
        values = []
        sql = 'delete from %s ' % table
        if len(conditions) > 0:
            sql += 'where'
            for k in conditions:
                sql += ' %s=%%s and ' % k
                values.append(conditions[k])
            sql = sql[:-5]
        sql += ';'

        return self._cur_execute(sql, values)

    def close(self):
        """Close connection
        """
        self.cur.close()
        self.con.close()
