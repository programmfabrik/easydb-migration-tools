'''
* easydb-migration-tools - easydb Migration Tools
 * Copyright (c) 2013 - 2016 Programmfabrik GmbH
 * MIT Licence
 * https://github.com/programmfabrik/coffeescript-ui, http://www.coffeescript-ui.org
'''

import logging
import sqlite3
from contextlib import closing

from easydb.repository.base import *

class SQLiteRepository(Repository):

    def __init__(self, dbname, filename):
        self.dbname = dbname
        self.filename = filename
        self.schema_def = None
        self.connected = False
        self.logger = logging.getLogger('easydb.repository.sqlite')

    def open(self):
        self.logger.debug('open')
        self.conn = sqlite3.connect(self.filename)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute('pragma foreign_keys=ON')
        self.connected = True

    def close(self):
        super(SQLiteRepository, self).close()
        self.logger.debug('close')
        self.conn.commit()
        self.conn.close()
        self.connected = False

    def is_open(self):
        return self.connected

    def get_schema_def(self):
        super(SQLiteRepository, self).get_schema_def()
        with closing(self.conn.cursor()) as cur:
            if self.schema_def is None:
                self.schema_def = SchemaDefinition(self.dbname)
                cur.execute("select name from sqlite_master where type = 'table'")
                table_names = []
                for r in cur:
                    table_names.append(r['name'])
                for name in table_names:
                    self.schema_def.tables.append(self.get_table_def(name))
            return self.schema_def

    def get_table_def(self, name):
        table_def = TableDefinition(name)
        with closing(self.conn.cursor()) as cur:
            cur.execute('pragma table_info("{0}")'.format(name))
            for r in cur:
                column_def = ColumnDefinition(r['name'])
                column_def.type = r['type']
                column_def.pk = r['pk']
                table_def.columns.append(column_def)
            return table_def

    def extract_table(self, table_name):
        schema_def = self.get_schema_def()
        for table_def in schema_def.tables:
            if table_def.name == table_name:
                order_column = None
                for column_def in table_def.columns:
                    if column_def.pk:
                        order_column = column_def.name
                order_clause = '' if order_column is None else ' order by "{0}"'.format(order_column)
                return self.execute('select * from "{0}"{1}'.format(table_name, order_clause))
        raise TableNotFoundError(table_name)

    def create_schema(self, schema_def):
        super(SQLiteRepository, self).create_schema(schema_def)

    def add_table(self, table_def):
        super(SQLiteRepository, self).add_table(table_def)
        sql_parts = list(map(lambda column_def : self._table_column(column_def), table_def.columns))
        sql_parts.extend(list(map(lambda constraint_def : self._table_constraint(constraint_def), table_def.constraints)))
        sql = 'create table "{0}" (\n\t{1}\n)'.format(table_def.name, ',\n\t'.join(sql_parts))
        self.logger.info('create table {0}'.format(table_def.name))
        self.logger.debug('execute:{0}\n'.format(sql))
        self.conn.execute(sql)

    def _table_column(self, column_def):
        extra_info = ''
        if column_def.pk:
            extra_info = ' primary key not null'
        return '"{0}" {1}{2}'.format(column_def.name, SQLiteRepository.translate_type(column_def.type), extra_info)

    def _table_constraint(self, constraint_def):
        if isinstance(constraint_def, UniqueConstraintDefinition):
            c = ', '.join(map(quote_name, constraint_def.columns))
            return 'unique ({0})'.format(c)
        elif isinstance(constraint_def, ForeignKeyConstraintDefinition):
            oc = ', '.join(map(quote_name, constraint_def.own_columns))
            rc = ', '.join(map(quote_name, constraint_def.ref_columns))
            extra = ''
            if constraint_def.on_delete is not None:
                extra += ' on delete {0}'.format(constraint_def.on_delete)
            if constraint_def.deferrable:
                extra += ' deferrable initially deferred'
            return 'foreign key ({0}) references {1} ({2}){3}'.format(oc, quote_name(constraint_def.ref_table_name), rc, extra)

    def insert_row(self, table_name, row):
        super(SQLiteRepository, self).insert_row(table_name, row)
        column_names = []
        column_values = []
        for (k,v) in list(row.items()):
            column_names.append(k)
            column_values.append(v)
        columns = ', '.join(map(quote_name, column_names))
        placeholders = ', '.join(['?'] * len(column_values))
        sql = 'insert into "{0}"\n({1})\nvalues\n({2})'.format(table_name, columns, placeholders)
        try:
            self.execute(sql, *column_values)
        except sqlite3.OperationalError as e:
            raise ExecutionError(str(e))
        except sqlite3.IntegrityError as e:
            raise ExecutionError(str(e))

    def update_row(self, table_name, row, condition):
        super(SQLiteRepository, self).update_row(table_name, row, condition)
        column_names = []
        column_values = []
        for column_name, value in row.items():
            column_names.append(column_name)
            column_values.append(value)
        set_instructions = ',\n'.join(map(lambda x : '{} = ?'.format(quote_name(x)), column_names))
        sql = 'update "{}"\nset\n{}\nwhere {}'.format(table_name, set_instructions, condition)
        try:
            self.execute(sql, *column_values)
        except sqlite3.OperationalError as e:
            raise ExecutionError(str(e))
        except sqlite3.IntegrityError as e:
            raise ExecutionError(str(e))

    def execute(self, sql, *parameters):
        super(SQLiteRepository, self).execute(sql)
        try:
            self.logger.info('execute:\n{0}\nwith parameters: {1}'.format(sql, parameters))
            cur = self.conn.cursor()
            cur.execute(sql, parameters)
            return SQLiteRowIterator(cur)
        except sqlite3.OperationalError as e:
            raise ExecutionError(str(e))
        except sqlite3.IntegrityError as e:
            raise ExecutionError(str(e))

    def __repr__(self):
        return 'SQLiteRepository({0}, {1})'.format(self.dbname, self.filename)

    @staticmethod
    def translate_type(ctype):
        if ctype == 'integer':
            return 'integer'
        else:
            return 'text'

class SQLiteRowIterator(object):

    def __init__(self, cur):
        self.cur = cur
        self.rowcount = self.cur.rowcount

    def __iter__(self):
        return self

    def __next__(self):
        row = self.cur.fetchone()
        if row is None:
            raise StopIteration
        result = {}
        for column_name in row.keys():
            result[column_name] = row[column_name]
        return result

    def get_rows(self):
        return self.cur.fetchall()    

    def next(self):
        return self.__next__()

    def __del__(self):
        self.cur.close()
