'''
* easydb-migration-tools - easydb Migration Tools
 * Copyright (c) 2013 - 2016 Programmfabrik GmbH
 * MIT Licence
 * https://github.com/programmfabrik/coffeescript-ui, http://www.coffeescript-ui.org
'''

__all__ = [
    'prepare',
    'CreatePolicy'
]

import logging
import os
import json

import easydb.server.datamodel
import easydb.migration.transform.destination
import easydb.migration.transform.source

logger = logging.getLogger('easydb.migration.transform.prepare')

# public

class CreatePolicy:
    Always = 'Always'
    IfNotExists = 'IfNotExists'
    Never = 'Never'

def prepare(easydb_api, destination_directory, source_directory, create_policy, user_tables=[]):
    logger.info('begin')
    logger.debug('using easydb: {0}'.format(easydb_api))
    logger.debug('using destination directory: {0}'.format(destination_directory))
    destination_schema = DestinationSchema(easydb_api, destination_directory)
    destination = easydb.migration.transform.destination.Destination(destination_directory, destination_schema)
    if not destination.exists() and create_policy == CreatePolicy.Never:
        raise Exception('destination does not exist')
    if not destination.exists() or create_policy == CreatePolicy.Always:
        logger.info('create')
        destination.create()
        for table_def in user_tables:
            destination.create_user_table(table_def)
    logger.info('end')
    source = easydb.migration.transform.source.Source(source_directory)
    return destination, source

# private

class DestinationSchema(object):

    def __init__(self, ezapi, destination_directory):
        schema_file = '{0}/schema.json'.format(destination_directory)
        l10n_file = '{0}/l10n.json'.format(destination_directory)
        if os.path.isfile(schema_file):
            logger.info('load schema from file')
            with open(schema_file, 'r') as f:
                self.ez_schema = easydb.server.datamodel.EasydbSchema.parse(json.load(f))
        else:
            logger.info('load schema from easydb')
            schema_js = easydb.server.datamodel.get_current_schema(ezapi)
            self.ez_schema = easydb.server.datamodel.EasydbSchema.parse(schema_js)
            with open(schema_file, 'w') as f:
                json.dump(schema_js, f, indent=4)
        if os.path.isfile(l10n_file):
            logger.info('load languages from file')
            with open(l10n_file, 'r') as f:
                self.ez_languages = json.load(f)
        else:
            logger.info('load languages from easydb')
            self.ez_languages = easydb.server.datamodel.load_schema_languages(ezapi)
            with open(l10n_file, 'w') as f:
                json.dump(self.ez_languages, f, indent=4)
        self.db_schema = easydb.server.datamodel.SchemaDefinition('destination')
        self.ez_to_db = {}
        self._create_common_tables()
        self._load_objecttypes()

    def create(self, db):
        db.create_schema(self.db_schema)

    def get_ez_schema(self):
        return self.ez_schema

    def get_table_for_objecttype(self, objecttype):
        return self.ez_to_db[objecttype]

    def get_schema_languages(self):
        return self.ez_languages

    def _create_common_tables(self):
        self._create_dependencies()
        self._create_pool()
        self._create_group()
        self._create_user()
        self._create_user__group()
        self._create_tag_group()
        self._create_tag()
        self._create_collection()
        self._create_collection_objects()

    def _create_dependencies(self):
        logger.debug('create dependencies table')
        table_def = easydb.server.datamodel.TableDefinition('dependencies')
        self._add_column(table_def, 'parent_table', 'text')
        self._add_column(table_def, 'parent_id', 'text')
        self._add_column(table_def, 'child_table', 'text')
        self._add_column(table_def, 'child_id', 'text')
        self.db_schema.tables.append(table_def)

    def _create_pool(self):
        logger.debug('create pool table')
        table_def = self._easydb_table('ez_pool')
        self._add_column(table_def, '__parent_id', 'text')
        self._add_l10n_columns(table_def, 'name')
        self._add_l10n_columns(table_def, 'description')
        self._add_column(table_def, '_standard_masks', 'text')
        self.db_schema.tables.append(table_def)


    def _create_collection(self):
        logger.debug('create collection table')
        table_def = self._easydb_table('ez_collection')
        self._add_l10n_columns(table_def, 'displayname')
        self._add_l10n_columns(table_def, 'description')
        self._add_column(table_def, '__parent_id', 'text')
        self._add_column(table_def, '__owner', 'text')
        self._add_column(table_def, '__type', 'text')
        self._add_column(table_def, '__owner_id', 'text')
        self._add_column(table_def, '__user_collection_id', 'text')
        self.db_schema.tables.append(table_def)

    def _create_collection_objects(self):
        logger.debug('create collection_objects table')
        table_def = self._easydb_table('ez_collection__objects')
        self._add_column(table_def, 'collection_id', 'text')
        self._add_column(table_def, 'collection_id_new', 'text')
        self._add_column(table_def, 'object_id', 'text')
        self._add_column(table_def, 'object_goid', 'text')
        self._add_column(table_def, 'uploaded', 'text')
        self._add_column(table_def, 'position', 'text')
        self.db_schema.tables.append(table_def)

    def _create_group(self):
        logger.debug('create group table')
        table_def = self._easydb_table('ez_group')
        self._add_column(table_def, 'name', 'text')
        self._add_l10n_columns(table_def, 'displayname')
        self._add_column(table_def, 'comment', 'text')
        self.db_schema.tables.append(table_def)

    def _create_user(self):
        logger.debug('create user table')
        table_def = self._easydb_table('ez_user')
        self._add_column(table_def, 'last_name', 'text')
        self._add_column(table_def, 'first_name', 'text')
        self._add_column(table_def, 'remarks', 'text')
        self._add_column(table_def, 'login', 'text')
        self._add_column(table_def, 'email', 'text')
        self._add_column(table_def, 'phone', 'text')
        self._add_column(table_def, 'street', 'text')
        self._add_column(table_def, 'postal_code', 'text')
        self._add_column(table_def, 'town', 'text')
        self._add_column(table_def, 'country', 'text')
        self._add_column(table_def, 'password', 'text')
        self._add_column(table_def, 'frontend_prefs', 'text')
        self._add_column(table_def, 'login_disabled', 'bool')
        self.db_schema.tables.append(table_def)

    def _create_user__group(self):
        logger.debug('create user__group table')
        table_def = self._easydb_table('ez_user__group')
        self._add_column(table_def, 'user_id', 'text')
        self._add_column(table_def, 'group_id', 'text')
        self.db_schema.tables.append(table_def)

    def _create_tag_group(self):
        logger.debug('create tag_group table')
        table_def = self._easydb_table('ez_tag_group')
        self._add_column(table_def, 'type', 'text')
        self._add_l10n_columns(table_def, 'displayname')
        self.db_schema.tables.append(table_def)

    def _create_tag(self):
        logger.debug('create tag table')
        table_def = self._easydb_table('ez_tag')
        self._add_column(table_def, 'type', 'text')
        self._add_column(table_def, 'displaytype', 'text')
        self._add_l10n_columns(table_def, 'displayname')
        self._add_column(table_def, 'group', 'text')
        table_def.constraints.append(easydb.server.datamodel.ForeignKeyConstraintDefinition(['group'], 'easydb.ez_tag_group', ['__source_unique_id']))
        self.db_schema.tables.append(table_def)

    def _load_objecttypes(self):
        for ot in self.ez_schema.objecttypes.values():
            self._load_objecttype(ot)

    def _load_objecttype(self, ot):
        logger.debug('create tables for objecttype "{0}"'.format(ot.name))
        table_def = self._easydb_table(ot.name)
        table_def.constraints = ot.constraints

        self._add_column(table_def, '__mask', 'text')
        self._add_column(table_def, '__comment', 'text')
        self._add_column(table_def, 'collection_id', 'text')
        if ot.pool_link:
            self._add_column(table_def, '__pool_id', 'text')
        if ot.is_hierarchical:
            self._add_column(table_def, '__parent_id', 'text')
            table_def.constraints.append(easydb.server.datamodel.ForeignKeyConstraintDefinition(['__parent_id'], ot.name, ['__source_unique_id'], True))
        if ot.owned_by is not None:
            self._add_column(table_def, '__uplink_id', 'text')
            table_def.constraints.append(easydb.server.datamodel.ForeignKeyConstraintDefinition(['__uplink_id'], ot.owned_by, ['__source_unique_id'], True))
        if ot.has_tags:
            self.db_schema.tables.append(self._tag_table(ot.name))
        for ez_column in ot.columns.values():
            self._load_column(table_def, ot, ez_column)

        for c in table_def.constraints:
            if isinstance(c, easydb.server.datamodel.ForeignKeyConstraintDefinition):
                c.ref_objecttype = c.ref_table_name
                c.ref_table_name = self._easydb_table_name(c.ref_table_name)

        self.db_schema.tables.append(table_def)
        self.ez_to_db[ot.name] = table_def

    def _load_column(self, table_def, ot, ez_column):
        if ez_column.kind == 'column':
            if ez_column.column_type == 'eas':
                self.db_schema.tables.append(self._asset_table(ot.name, ez_column.name))
            elif 'l10n' in ez_column.column_type:
                self._add_l10n_columns(table_def, ez_column.name)
            else:
                self._add_column(table_def, ez_column.name, ez_column.column_type)

    def _asset_table(self, ot_name, column_name):
        table_def = easydb.server.datamodel.TableDefinition('asset.{0}.{1}'.format(ot_name, column_name))
        column_def = easydb.server.datamodel.ColumnDefinition('__source_unique_id')
        column_def.type = 'text'
        column_def.pk = True
        table_def.columns.append(column_def)
        self._add_column(table_def, '__version', 'integer')
        self._add_column(table_def, '__eas_id', 'integer')
        self._add_column(table_def, 'object_id', 'text')
        self._add_column(table_def, 'preferred', 'integer')
        self._add_column(table_def, 'original_filename', 'text')
        self._add_column(table_def, 'source_type', 'text')
        self._add_column(table_def, 'source', 'text')
        fk = easydb.server.datamodel.ForeignKeyConstraintDefinition(['object_id'], self._easydb_table_name(ot_name), ['__source_unique_id'])
        table_def.constraints = [fk]
        return table_def

    def _tag_table(self, ot_name):
        table_def = easydb.server.datamodel.TableDefinition('tag.{0}'.format(ot_name))
        column_def = easydb.server.datamodel.ColumnDefinition('__source_unique_id')
        column_def.type = 'text'
        column_def.pk = True
        table_def.columns.append(column_def)
        self._add_column(table_def, '__version', 'integer')
        self._add_column(table_def, 'object_id', 'text')
        self._add_column(table_def, 'tag_id', 'text')
        fk1 = easydb.server.datamodel.ForeignKeyConstraintDefinition(['object_id'], self._easydb_table_name(ot_name), ['__source_unique_id'])
        fk2 = easydb.server.datamodel.ForeignKeyConstraintDefinition(['tag_id'], 'easydb.ez_tag', ['__source_unique_id'])
        table_def.constraints = [fk1, fk2]
        return table_def

    def _easydb_table(self, name):
        table_def = easydb.server.datamodel.TableDefinition(self._easydb_table_name(name))
        column_def = easydb.server.datamodel.ColumnDefinition('__source_unique_id')
        column_def.type = 'text'
        column_def.pk = True
        table_def.columns.append(column_def)
        self._add_column(table_def, '__easydb_id', 'integer')
        self._add_column(table_def, '__easydb_goid', 'text')
        self._add_column(table_def, '__version', 'integer')
        return table_def

    def _easydb_table_name(self, name):
        return 'easydb.{0}'.format(name)

    def _add_l10n_columns(self, table_def, name):
        for language in self.ez_languages:
            self._add_column(table_def, '{0}:{1}'.format(name, language), 'text')

    def _add_column(self, table_def, name, ctype):
        column_def = easydb.server.datamodel.ColumnDefinition(name)
        column_def.type = ctype
        table_def.columns.append(column_def)
