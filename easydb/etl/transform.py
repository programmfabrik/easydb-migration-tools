import logging
import abc

class Transformer(object):

    def __init__(self, transformations):
        self.transformations = transformations

    def transform_row(self, schema_table_name, row):
        logger = logging.getLogger('easydb.etl.transform')
        for (column_name, value) in list(row.items()):
            for t in self.transformations.get_column_transformations(schema_table_name, column_name):
                logger.debug('{0}.{1}: apply column transformation {2}'.format(schema_table_name, column_name, t))
                row[column_name] = t.transform(row[column_name])
        for t in self.transformations.get_row_transformations(schema_table_name):
            logger.debug('{0}: apply row transformation {1}'.format(schema_table_name, t))
            row = t.transform(row)

class Transformations(object):

    def __init__(self):
        self.column_transformations = {}
        self.row_transformations = {}

    def add_column_transformation(self, origin_db_name, origin_table, column_name, transformation):
        key = '{0}.{1}.{2}'.format(origin_db_name, origin_table, column_name)
        if key not in self.column_transformations:
            self.column_transformations[key] = []
        self.column_transformations[key].append(transformation)

    def get_column_transformations(self, schema_table_name, column_name):
        key = '{0}.{1}'.format(schema_table_name, column_name)
        if key not in self.column_transformations:
            return []
        else:
            return self.column_transformations[key]

    def add_row_transformation(self, origin_db_name, origin_table, transformation):
        key = '{0}.{1}'.format(origin_db_name, origin_table)
        if key not in self.row_transformations:
            self.row_transformations[key] = []
        self.row_transformations[key].append(transformation)

    def get_row_transformations(self, schema_table_name):
        if schema_table_name not in self.row_transformations:
            return []
        else:
            return self.row_transformations[schema_table_name]

    def __repr__(self):
        result = '* Column transformations:'
        for (k, v) in list(self.column_transformations.items()):
            result += '\n   - {0}: {1}'.format(k, v)
        result += '* Row transformations:'
        for (k, v) in list(self.row_transformations.items()):
            result += '\n   - {0}: {1}'.format(k, v)
        return result

class ColumnTransformation(object):
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def transform(self, value):
        return

class RowTransformation(object):
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def transform(self, row):
        return
