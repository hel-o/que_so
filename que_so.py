# coding=utf-8
"""
Que-so is a fast, easy and simple way for make CRUD, perfect for write proof of concepts in a fast way
and other small applications using sqlite as database,
all in a single file and with no third dependencies than the Python Standard Library.
"""

import sqlite3 as lite
from datetime import datetime

__author__ = 'Helo'
__version__ = '0.1'
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

    _fields_with_value = []
    _fields_value = []
    _form_fields = {}
    _select_fields = '*'
    _select_filter = None

    def __init__(self):
        if not self.__database__:
            self.__database__ = '{}.db'.format(self.__class__.__name__.lower())
        
        if not self.__table__:
            self.__table__ = self.__class__.__name__.lower()

        # for the static context:
        Model.__table__ = self.__table__
        Model.__database__ = self.__database__

        self._form_fields = self._get_fields()

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

    def _get_fields(self):
        """check the fields and match with its data type according to:
         https://www.sqlite.org/datatype3.html"""
        fields = {}
        for attr in dir(self):
            if not callable(getattr(self, attr)) and not attr.startswith('_') and attr not in self._data_types:
                attr_type = type(getattr(self, attr))
                if attr_type == int:
                    fields[attr] = 'INTEGER'
                elif attr_type == float:
                    fields[attr] = 'REAL'
                elif attr_type == str:
                    fields[attr] = 'TEXT'
                elif attr_type == bool:
                    fields[attr] = 'NUMERIC'
                elif attr_type == datetime:
                    fields[attr] = 'NUMERIC'
                else:
                    fields[attr] = ''
        return fields

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

    @staticmethod
    def select(fields='*'):
        Model._select_fields = fields
        return Model

    @staticmethod
    def filter(where=None, order_by=None):
        sql_filter = None
        if where:
            sql_filter = ' WHERE {}'.format(where)
        if order_by:
            if where:
                sql_filter = ' {0} ORDER BY {1}'.format(sql_filter, order_by)
            else:
                sql_filter = ' ORDER BY {}'.format(order_by)
        sql_select = '{0} {1}'.format(Model._make_select(), sql_filter)
        return Model._execute_select_or_delete(sql_select)

    @staticmethod
    def all():
        sql_select = Model._make_select()
        return Model._execute_select_or_delete(sql_select)

    @staticmethod
    def _make_select():
        return 'SELECT {0} FROM {1}'.format(Model._select_fields, Model.__table__)

    @staticmethod
    def delete(where=None):
        """returns de rows affected"""
        sql_delete = 'DELETE FROM {}'.format(Model.__table__)
        if where:
            sql_delete = '{0} WHERE {1}'.format(sql_delete, where)
        return Model._execute_select_or_delete(sql_delete, for_delete=True)

    @staticmethod
    def _execute_select_or_delete(sql, for_delete=False):
        cn = lite.connect(Model.__database__)
        with cn:
            cursor = cn.cursor()
            cursor.execute(sql)
            if for_delete:
                return cursor.rowcount
            return cursor.fetchall()
