'''
* easydb-migration-tools - easydb Migration Tools
 * Copyright (c) 2013 - 2016 Programmfabrik GmbH
 * MIT Licence
 * https://github.com/programmfabrik/coffeescript-ui, http://www.coffeescript-ui.org
'''

import os
import logging

import easydb.repository.sqlite

class Destination(object):

    def __init__(self, directory, schema):
        self.logger = logging.getLogger('easydb.etl.destination')
        self.directory = directory
        if not os.path.exists(self.directory):
            os.makedirs(self.directory)
        self.filename = '{0}/destination.db'.format(self.directory)
        self.schema = schema
        self.preloaded_assets = None
        preloaded_assets_file = '{0}/preloaded_assets.txt'.format(self.directory)
        if os.path.exists(preloaded_assets_file):
            self.preloaded_assets = set()
            with open(preloaded_assets_file, 'r') as f:
                for line in iter(f):
                    self.preloaded_assets.add(int(line))
            self.logger.info('preloaded assets: {0}'.format(len(self.preloaded_assets)))

    def exists(self):
        return os.path.exists(self.filename)

    def create(self):
        try:
            os.remove(self.filename)
        except OSError:
            pass
        db = self.get_db()
        db.open()
        self.schema.create(db)
        db.close()

    def get_db(self):
        return easydb.repository.sqlite.SQLiteRepository('destination', self.filename)

    def get_ez_schema(self):
        return self.schema.get_ez_schema()

    def get_table_for_objecttype(self, objecttype):
        return self.schema.get_table_for_objecttype(objecttype)

    def get_schema_languages(self):
        return self.schema.get_schema_languages()

    def create_user_table(self, table_def):
        db = self.get_db()
        db.open()
        table_def.name = 'user.{0}'.format(table_def.name)
        db.add_table(table_def)
        db.close()

    def consume_asset(self, eas_id):
        if self.preloaded_assets is None:
            return True
        if eas_id in self.preloaded_assets:
            self.preloaded_assets.remove(eas_id)
            return True
        return False

    # def prepare_dependencies(self):
    #     db = easydb.repository.sqlite.SQLiteRepository('destination', self.filename)
    #     db.open()
    #     db.execute('delete from dependencies')
    #     db.execute('vacuum')
    #     for (_, ot) in self.ez_schema.objecttypes.items():
    #         self.prepare_dependencies__ot(ot, db)
    #     db.close()

    # def prepare_dependencies__ot(self, ot, db):
    #     table_def = self.ez_to_db[ot.name]
    #     sql_columns = []
    #     sql_joins = []
    #     sql_column_to_table = {}
    #     count = 1
    #     if ot.is_hierarchical:
    #         sql = 'select "__source_unique_id", "__parent_id" from "{0}" where "__parent_id" is not null'.format(table_def.name)
    #         rows = db.execute(sql)
    #         for row in rows:
    #             sql = 'insert into dependencies (parent_table, parent_id, child_table, child_id) values (?, ?, ?, ?)'
    #             db.execute(sql, table_def.name, row["__parent_id"], table_def.name, row["__source_unique_id"])
    #         del(rows)
    #     for constraint_def in table_def.constraints:
    #         if isinstance(constraint_def, ForeignKeyConstraintDefinition):
    #             # FIXME: support only 1-to-1 fks
    #             if len(constraint_def.own_columns) != 1 or len(constraint_def.ref_columns) != 1:
    #                 raise Exception('fks with more than one column not yet supported')
    #             oc = quote_name(constraint_def.own_columns[0])
    #             rt = quote_name(constraint_def.ref_table_name)
    #             rc = quote_name(constraint_def.ref_columns[0])
    #             sql_column = 'f{0}'.format(count)
    #             sql_column_to_table[sql_column] = constraint_def.ref_table_name
    #             sql_columns.append('t{0}."__source_unique_id" as f{0}'.format(count, sql_column))
    #             sql_joins.append('left join {0} t{1} on t{1}.{2} = t0.{3}'.format(rt, count, rc, oc))
    #             count += 1
    #     if len(sql_columns) == 0:
    #         return
    #     sql_columns.append('t0."__source_unique_id" as f0')
    #     sql = 'select {0}\nfrom {1} t0\n{2}'.format(', '.join(sql_columns), quote_name(table_def.name), '\n'.join(sql_joins))
    #     rows = db.execute(sql)
    #     for row in rows:
    #         parent_id = row['f0']
    #         for i in range(1, count):
    #             column = 'f{0}'.format(i)
    #             child_id = row[column]
    #             if child_id is not None:
    #                 sql = 'insert into dependencies (parent_table, parent_id, child_table, child_id) values (?, ?, ?, ?)'
    #                 db.execute(sql, table_def.name, parent_id, sql_column_to_table[column], child_id)


