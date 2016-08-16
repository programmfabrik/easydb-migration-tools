import logging
import re

from easydb.etl.repository.base import quote_name, TableNotFoundError
from easydb.etl.repository.sqlite import SQLiteRepository

class StatementReplacementError(Exception):
    def __init__(self, error_str):
        self.error_str = error_str
        Exception.__init__(self, error_str)

def check_open(f):
    def new_f(self, *args, **kwargs):
        if not self.is_open():
            raise Exception('Source is not open')
        return f(self, *args, **kwargs)
    return new_f

class Source(object):

    def __init__(self, directory):
        self.directory = directory
        self.filename = '{0}/source.db'.format(self.directory)
        self.asset_dir = '{0}/assets'.format(self.directory)
        self.logger = logging.getLogger('easydb.etl.source')
        self.db = None

    def __del__(self):
        if self.is_open():
            self.close()

    def open(self):
        self.db = SQLiteRepository('source', self.filename)
        self.logger.info('open repository: {0}'.format(self.db))
        self.db.open()
        self.logger.info('extract metadata')
        self.metadata = Metadata.extract(self.db)
        self.table_map = {}
        for origin in self.metadata.origins:
            self.table_map[origin.get_name()] = origin.get_source_table()

    def is_open(self):
        return self.db is not None and self.db.is_open()

    @check_open
    def close(self):
        self.db.close()

    @check_open
    def execute(self, sql, *parameters):
        return self.db.execute(self.replace_statement(sql), *parameters)

    def replace_statement(self, sql):
        match = re.search('\[\[(\S*)\]\]', sql)
        if not match:
            return sql
        begin = sql[:match.start()]
        replace = sql[match.start(1):match.end(1)]
        rest = sql[match.end():]
        return begin + self.replace_variable(replace) + self.replace_statement(rest)

    def replace_variable(self, variable):
        parts = variable.split(':')
        if len(parts) != 2:
            raise Exception('variable "{0}" not valid: expecting {{table|column}}:<name>'.format(variable))
        if parts[0] == 'table':
            return self.replace_table_name(parts[1])
        elif parts[0] == 'column':
            return self.replace_column_name(parts[1])
        else:
            raise StatementReplacementError('variable type "{0}" not found'.format(parts[0]))

    def replace_table_name(self, table_name, quoted=True):
        if table_name in self.table_map:
            name = self.table_map[table_name]
            if quoted:
                name = quote_name(name)
            return name
        else:
            raise StatementReplacementError('table "{0}" not found'.format(table_name))

    def replace_column_name(self, column_name):
        # TODO
        return quote_name(column_name)

class Metadata(object):

    def __init__(self):
        self.origins = []

    def __repr__(self):
        return '* Origins:\n  {0}'.format('\n  '.join(map(repr, self.origins)))

    @staticmethod
    def extract(repository):
        logger = logging.getLogger('easydb.etl.source.metadata')
        schema_def = repository.get_schema_def()
        logger.info('extract from schema {0}'.format(schema_def.name))
        md = Metadata()
        try:
            for row in repository.extract_table('origin'):
                md.origins.append(Origin(row, schema_def))
        except TableNotFoundError:
            logger.error('table origin not found')
            raise Exception('extract metadata failed')
        except KeyError as e:
            logger.error('column {0} from table origin not found'.format(e))
            raise Exception('extract metadata failed')
        logger.info('Metadata:\n\n{0}'.format(repr(md)))
        return md

class Origin(object):

    def __init__(self, row, schema_def):
        self.origin_id = row['origin_id']
        self.origin_type = row['origin_type']
        self.origin_db_name = row['origin_database_name']
        self.origin_table = row['origin_table_name']
        self.source_name = row['source_name']
        self.source_table_name = row['source_table_name']
        source_table = self.get_source_table()
        for table_def in schema_def.tables:
            if table_def.name == source_table:
                self.table_def = table_def
                break
        else:
            logger.error('table {0} referenced by origin was not found'.format(source_table))
            raise Exception('extract metadata failed')

    def get_name(self):
        return '.'.join([self.origin_db_name, self.origin_table])
    def get_source_table(self):
        return '.'.join([self.source_name, self.source_table_name])

    def __repr__(self):
        return '{0} - {1} - {2}: {3}'.format(self.origin_id, self.origin_type, self.get_name(), self.get_source_table())
