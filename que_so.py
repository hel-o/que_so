# coding=utf-8
"""
Que-so is a fast, easy and simple way for make CRUD, perfect for write proof of concepts in a fast way
and other small applications using sqlite as database,
all in a single file and with no third dependencies than the Python Standard Library.
"""

import sqlite3 as lite
from datetime import datetime

__author__ = 'Helo'
__version__ = '0.1.2'
__license__ = 'BSD'


class Model(object):
    # data types available (variable values are only referential, will not be taken):
    integer = 0
    text = ''
    real = 0.0
    boolean = True
    datetime = datetime.now()

    # register the model data types:
    _data_types = ('integer', 'text', 'real', 'boolean', 'datetime')

    __database__ = None
    __table__ = None

    _form_fields = {}
    _select_fields = '*'
    _select_filter = None

    def __init__(self, dummy=False):
        self._fields_with_value = []
        self._fields_value = []

        if dummy:
            return

        self._check_config()
        self._search_form_fields()

        sql_fields = ''
        for field, type_field in self._form_fields.iteritems():
            sql_fields += '{0} {1}, '.format(field, type_field)

        sql_script = 'CREATE TABLE IF NOT EXISTS {0} ( {1} );'.format(self.__table__, sql_fields[:-2])

        cn = lite.connect(self.__database__)
        with cn:
            cursor = cn.cursor()
            cursor.executescript(sql_script)

    def __setattr__(self, key, value):
        super(Model, self).__setattr__(key, value)
        if key in self._form_fields:
            self._fields_with_value.append(key)
            self._fields_value.append(value)

    def _search_form_fields(self):
        """check the fields and match with its data type according to:
         https://www.sqlite.org/datatype3.html"""
        for attr in dir(self):
            if not callable(getattr(self, attr)) and not attr.startswith('_') and attr not in self._data_types:
                attr_type = type(getattr(self, attr))
                if attr_type == int:
                    self._form_fields[attr] = 'INTEGER'
                elif attr_type == float:
                    self._form_fields[attr] = 'REAL'
                elif attr_type == str:
                    self._form_fields[attr] = 'TEXT'
                elif attr_type == bool:
                    self._form_fields[attr] = 'NUMERIC'
                elif attr_type == datetime:
                    self._form_fields[attr] = 'NUMERIC'
                else:
                    self._form_fields[attr] = ''

    def save(self):
        """returns sqlite rowid"""
        fields = '?' * len(self._fields_with_value)
        sql_insert = 'INSERT INTO {0}({1}) VALUES({2});'.format(
            self.__table__,
            ', '.join(self._fields_with_value),
            ', '.join(fields)
        )
        return self._execute_save_or_update(sql_insert)

    def update(self, where=None):
        """returns the rows affected"""
        sql_update = 'UPDATE {} SET '.format(self.__table__)
        for field in self._fields_with_value:
            sql_update += '{0}=?, '.format(field)

        sql_update = sql_update[:-2]
        if where:
            sql_update = '{0} WHERE {1}'.format(sql_update, where)

        return self._execute_save_or_update(sql_update, for_update=True)

    def _execute_save_or_update(self, sql, for_update=False):
        cn = lite.connect(self.__database__)
        with cn:
            cursor = cn.cursor()
            cursor.execute(sql, self._fields_value)
            if for_update:
                return cursor.rowcount
            else:
                return cursor.lastrowid

    @classmethod
    def select(cls, fields='rowid, *'):
        cls._select_fields = fields
        return cls

    @classmethod
    def filter(cls, where=None, order_by=None):
        sql_filter = None
        if where:
            sql_filter = ' WHERE {}'.format(where)
        if order_by:
            if where:
                sql_filter = ' {0} ORDER BY {1}'.format(sql_filter, order_by)
            else:
                sql_filter = ' ORDER BY {}'.format(order_by)
        sql_select = '{0} {1}'.format(cls._make_select(), sql_filter)
        return cls._execute_select_or_delete(sql_select)

    @classmethod
    def all(cls):
        sql_select = cls._make_select()
        try:
            items = cls._execute_select_or_delete(sql_select)
        except lite.OperationalError as e:
            if 'no such table' in e.message:
                return []
            raise e
        else:
            items_base = []
            for i in items:
                base = Model(dummy=True)
                for k in i.keys():
                    base.__setattr__(k, i[k])
                items_base.append(base)
            return items_base

    @classmethod
    def _make_select(cls):
        cls._check_config()
        return 'SELECT {0} FROM {1}'.format(cls._select_fields, cls.__table__)

    @classmethod
    def delete(cls, where=None):
        """returns de rows affected"""
        cls._check_config()
        sql_delete = 'DELETE FROM {}'.format(cls.__table__)
        if where:
            sql_delete = '{0} WHERE {1}'.format(sql_delete, where)
        return cls._execute_select_or_delete(sql_delete, for_delete=True)

    @classmethod
    def _execute_select_or_delete(cls, sql, for_delete=False):
        cn = lite.connect(cls.__database__)
        with cn:
            cn.row_factory = lite.Row
            cursor = cn.cursor()
            cursor.execute(sql)
            if for_delete:
                return cursor.rowcount
            return cursor.fetchall()

    @classmethod
    def _check_config(cls):
        if not cls.__database__:
            cls.__database__ = '{}.db'.format(cls.__name__.lower())
        if not cls.__table__:
            cls.__table__ = cls.__name__.lower()
