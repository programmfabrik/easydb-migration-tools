import logging
import json

from easydb.repository.base import *
from easydb.tool.json import *

def get_current_schema(ezapi):
    if not ezapi.is_authenticated():
        raise Exception('EasydbDatamodel called with unauthenticated EasydbAPI')
    logger = logging.getLogger('easydb.server.datamodel')
    logger.info('load datamodel')
    return ezapi.get('schema/user/CURRENT', {'format': 'json'})

def load_schema_languages(ezapi):
    if not ezapi.is_authenticated():
        raise Exception('EasydbDatamodel called with unauthenticated EasydbAPI')
    logger = logging.getLogger('easydb.server.datamodel')
    logger.info('load schema languages')
    ez_config = ezapi.get('config/list')
    if 'system' not in ez_config:
        raise Exception('config has no "system" key')
    for var in ez_config['system']:
        if 'name' in var and var['name'] == 'languages':
            return extract_from_json(var, 'parameters.database.choices')
    raise Exception('config["system"] has no "languages" variable')

class EasydbSchema(object):

    def __init__(self, schema_type, version):
        self.schema_type = schema_type
        self.version = version
        self.objecttypes = {}

    @staticmethod
    def parse(js):
        logger = logging.getLogger('easydb.server.datamodel.schema')
        v = parse_json(js, {'type': '$type', 'version': '$version', 'tables': '$tables'})
        logger.info('load easydb {0} schema version {1} with {2} tables'.format(v['type'], v['version'], len(v['tables'])))
        schema = EasydbSchema(v['type'], v['version'])
        for table in v['tables']:
            ot = EasydbObjecttype.parse(table)
            schema.objecttypes[ot.name] = ot
        return schema

class EasydbObjecttype(object):
    _template = {
        'name': '$name',
        'columns': '$columns',
        '$pool_link': '$pool_link',
        '$unique_keys': '$unique_keys',
        '$foreign_keys': '$foreign_keys',
        '$is_hierarchical': '$is_hierarchical',
        '$has_tags': '$has_tags',
        '$owned_by': {
            'other_table_name_hint': '$owner'
        }
    }
    _fk_template = {
        'columns': '$own_columns',
        'referenced_table': {
            'name_hint': '$ref_table_name',
            'columns': '$ref_columns'
        }
    }
    _unique_template = {
        'columns': '$columns'
    }

    def __init__(self, name):
        self.name = name
        self.columns = {}
        self.constraints = []
        self.pool_link = False
        self.is_hierarchical = False
        self.owned_by = None
        self.has_tags = False

    @staticmethod
    def parse(js):
        logger = logging.getLogger('easydb.server.datamodel.objecttype')
        v = parse_json(js, EasydbObjecttype._template)
        logger.info('load table {0} with {1} columns'.format(v['name'], len(v['columns'])))
        ot = EasydbObjecttype(v['name'])
        for column in v['columns']:
            c = EasydbColumn.parse(column)
            if c is not None:
                ot.columns[c.name] = c
        if 'unique_keys' in js:
            for uk in js['unique_keys']:
                EasydbObjecttype.parse__unique(uk, ot)
        if 'foreign_keys' in v:
            for fk in v['foreign_keys']:
                EasydbObjecttype.parse__foreign_key(fk, ot)
        if 'is_hierarchical' in v:
            ot.is_hierarchical = v['is_hierarchical']
        if 'pool_link' in v:
            ot.pool_link = v['pool_link']
        if 'owner' in v:
            ot.owned_by = v['owner']
        if 'has_tags' in v:
            ot.has_tags = v['has_tags']
        return ot

    @staticmethod
    def parse__column(js):
        if 'column_name_hint' in js:
            return js['column_name_hint']
        elif 'auto_column_primary_key' in js:
            return '__source_unique_id'
        else:
            # FIXME: "auto_column_uplink"
            raise Exception('COLUMN NOT FOUND: ', json.dumps(js, indent=4))

    @staticmethod
    def parse__foreign_key(js, ot):
        v = parse_json(js, EasydbObjecttype._fk_template)
        own_columns = list(map(EasydbObjecttype.parse__column, v['own_columns']))
        ref_columns = list(map(EasydbObjecttype.parse__column, v['ref_columns']))
        ot.constraints.append(ForeignKeyConstraintDefinition(own_columns, v['ref_table_name'], ref_columns))

    @staticmethod
    def parse__unique(js, ot):
        try:
            v = parse_json(js, EasydbObjecttype._unique_template)
            columns = list(map(EasydbObjecttype.parse__column, v['columns']))
            ot.constraints.append(UniqueConstraintDefinition(columns))
        except Exception:
            # FIXME: see above
            pass

class EasydbColumn(object):

    @staticmethod
    def parse(js):
        logger = logging.getLogger('easydb.server.datamodel.column')
        v = parse_json(js, {'kind': '$kind'})
        if v['kind'] == 'column':
            v = parse_json(js, {'name': '$name', 'type': '$type'})
            column =  EasydbColumn()
            column.kind = 'column'
            column.name = v['name']
            column.column_type = v['type']
            logger.info('load column {0} ({1})'.format(column.name, column.column_type))
            return column
        if v['kind'] == 'link':
            v = parse_json(js, {'other_table_name_hint': '$other_table'})
            column =  EasydbColumn()
            column.kind = 'link'
            column.other_table = v['other_table']
            column.name = '_nested:{0}'.format(column.other_table)
            return column
        return None

