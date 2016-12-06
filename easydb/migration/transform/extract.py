'''
* easydb-migration-tools - easydb Migration Tools
 * Copyright (c) 2013 - 2016 Programmfabrik GmbH
 * MIT Licence
 * https://github.com/programmfabrik/coffeescript-ui, http://www.coffeescript-ui.org
'''

__all__ = [
    'extract',
    'Extractor',
    'RowTransformation',
    'AssetColumn'
]

import logging
import abc
import json
import sqlite3

import easydb.migration.transform.common
import easydb.tool.batch
import easydb.repository.base

logger = logging.getLogger('easydb.migration.transform.extract')
asset_types = ['data', 'filename', 'url']

# public

class Extractor(object):
    __metaclass__ = abc.ABCMeta
    @abc.abstractmethod
    def extract(self):
        return None
    def __str__(self):
        return 'Extractor'

class RowTransformation(object):
    __metaclass__ = abc.ABCMeta
    @abc.abstractmethod
    def transform(self, row):
        return None
    def __str__(self):
        return 'RowTransformation'

class AssetColumn(object):
    def __init__(self, schema, table, column, destination_table, destination_column, preferred_types=[]):
        self.schema = schema
        self.table = table
        self.column = column
        self.destination_table = destination_table
        self.destination_column = destination_column
        self.preferred_types = []
        for t in preferred_types:
            if t not in asset_types:
                raise Exception('asset type "{0}" does not exist'.format(t))
            self.preferred_types.append(t)
        for t in asset_types:
            if t not in self.preferred_types:
                self.preferred_types.append(t)
    def __str__(self):
        return '{0}.{1}.{2} -> {0}.{1}'.format(self.schema, self.table, self.column, self.destination_table, self.destination_column)

def extract(
    source,
    destination,
    extractor,
    destination_table,
    row_transformations = [],
    asset_columns = [],
    batch_size = 1000,
    stop_on_error=True,
    defer_foreign_keys=False):

    success = True
    manage_source = not source.is_open()
    if manage_source:
        source.open()
    db = destination.get_db()
    db.open()
    logger.info('begin')
    if defer_foreign_keys:
        logger.info('defer foreign keys')
        db.execute('PRAGMA defer_foreign_keys = true');
    try:
        process(db, source, destination, extractor, destination_table, row_transformations, asset_columns, batch_size, stop_on_error)
    except easydb.migration.transform.common.MigrationStop:
        success = False
    try:
        db.close()
    except sqlite3.IntegrityError as e:
        # sqlite does not give information, but this should be the __parent_id constraint
        logger.error('Integrity error: {0}'.format(e))
        logger.error('most probably: __parent_id / __uplink_id constraint violated')
        success = False
    logger.info('end')
    if manage_source:
        source.close()
    if not success:
        raise easydb.migration.transform.common.MigrationStop()

# private

def process(db, source, destination, extractor, destination_table, row_transformations, asset_columns, batch_size, stop_on_error):
    p = '[{0}]'.format(extractor)
    logger.info('{0} begin'.format(p))
    rows = extractor.extract()
    job = easydb.tool.batch.BatchedJob(easydb.tool.batch.BatchMode.Dictionary, batch_size, process_batch, db, source, destination_table, asset_columns, stop_on_error, p)
    success = True
    try:
        for row in rows:
            pb = '{0}[{1}]'.format(p, job.batch_nr)
            logger.debug('{0} row - original: {1}'.format(pb, row))
            rs = [row]
            for t in row_transformations:
                rs = t.transform(row)
                if rs is None:
                    logger.debug('{0} row - skipped ({1})'.format(pb, t, row))
                    rs = []
                    break
                if not isinstance(rs, list):
                    rs = [rs]
                for r in rs:
                    logger.debug('{0} row - transformed ({1}): {2}'.format(pb, t, r))
            for r in rs:
                if '__source_unique_id' not in r:
                    logger.error('{0} row does not contain "__source_unique_id"'.format(pb))
                    if stop_on_error:
                        raise easydb.migration.transform.common.MigrationStop()
                    else:
                        continue
                if '__version' not in r:
                    r['__version'] = 1
                source_id = str(r['__source_unique_id'])
                job.add(r, source_id)
        job.finish()
    except easydb.migration.transform.common.MigrationStop:
        success = False
    del(rows)
    logger.info('{0} end'.format(p))
    if not success:
        raise easydb.migration.transform.common.MigrationStop()

def process_batch(batch, db, source, destination_table, asset_columns, stop_on_error, p):
    source_ids = batch.keys()
    source_ids_str = '\',\n\t\''.join(map(lambda x: x.replace("'", "''"), source_ids))
    check_sql = SQL_check_if_exists.format(destination_table, source_ids_str)
    check_rows = db.execute(check_sql)
    update_ids = set()
    for row in check_rows:
        suid = row['__source_unique_id']
        new_version = batch[suid]['__version']
        old_version = row['__version']
        if old_version == None:
            # compatibility with old destinations
            old_version = 1
        if new_version == old_version:
            del batch[suid]
        elif old_version > new_version:
            logger.error('{} old_version ({}) > new_version ({})'.format(suid, old_version, new_version))
            if stop_on_error:
                raise easydb.migration.transform.common.MigrationStop()
            else:
                del batch[suid]
        update_ids.add(suid)
    del(check_rows)
    success = True
    logger.info('{} insert/update {} objects'.format(p, len(batch)))
    try:
        for source_id, row in batch.items():
            try:
                file_table_id = source_id
                if '__file_table_id' in row:
                    file_table_id = row['__file_table_id']
                    del row['__file_table_id']
                if source_id in update_ids:
                    db.update_row(destination_table, row, "__source_unique_id = '{}'".format(source_id))
                    # TODO: also allow to update assets
                else:
                    db.insert_row(destination_table, row)
                    for asset_column in asset_columns:
                        process_assets(db, source, asset_column, file_table_id, source_id, stop_on_error)
            except easydb.repository.base.ExecutionError as e:
                logger.error('error when inserting/updating row to {0}:\n{1}\n{2}'.format(destination_table, json.dumps(row, indent=4), e))
                if stop_on_error:
                    raise easydb.migration.transform.common.MigrationStop()
            except Exception as e:
                logger.error(e)
                if stop_on_error:
                    raise easydb.migration.transform.common.MigrationStop()
    except easydb.migration.transform.common.MigrationStop:
        success = False
    if not success:
        raise easydb.migration.transform.common.MigrationStop()

def process_assets(db, source, asset_column, file_table_id, source_id, stop_on_error):
    try:
        logger.debug('process_assets {0} for {1}'.format(asset_column, source_id))
        error_str = 'fetching rows'
        rows = source.execute(
            SQL_get_asset_info,
            asset_column.schema,
            asset_column.table,
            asset_column.column,
            file_table_id)
        assets = {}
        for row in rows:
            for t in asset_column.preferred_types:
                if row['has_{0}'.format(t)]:
                    source_type = t
                    break
            else:
                logger.error('filestore: no valid source: {0} -> {1}'.format(asset_column, file_table_id))
            asset = {
                'parent': row['eas_root_id'],
                'row': {
                    'object_id': source_id,
                    'preferred': False,
                    'original_filename': row['original_filename'],
                    'source_type': source_type
                }
            }
            if source_type == 'data':
                asset['row']['source'] = row['filestore_id']
            else:
                asset['row']['source'] = row[source_type]
            file_id = row['eas_id']
            if file_id is None:
                file_id = row['file_id']
            assets[file_id] = asset
            asset['row']['__source_unique_id'] = '{0}:{1}:{2}'.format(source_id, source_type, asset['row']['source'])
        asset_list = build_asset_list(assets)
        if len(asset_list) > 0:
            asset_list[0]['row']['preferred'] = True
            table = 'asset.{0}.{1}'.format(asset_column.destination_table, asset_column.destination_column)
            for asset in asset_list:
                row = asset['row']
                error_str = 'inserting asset {0}'.format(row['source'])
                rs = db.execute('select count(*) from "{0}" where __source_unique_id = \'{1}\''.format(table, row['__source_unique_id']))
                if rs.next()['count(*)'] == 0:
                    db.insert_row(table, row)
                del rs
    except easydb.repository.base.ExecutionError as e:
        logger.error('error when {4} for {0}.{1}.{2}:{3}\nERROR: {4}'.format(asset_column.schema, asset_column.table, asset_column.column, source_id, e, error_str))
        raise Exception('extract - could not fetch assets: {0}'.format(e))

def build_asset_list(assets):
    first_asset_id = None
    for asset_id, asset in assets.items():
        if asset['parent'] is None or asset['parent'] not in assets:
            first_asset_id = asset_id
            break
    if first_asset_id is None:
        return list(assets.items())
    first_asset = assets[first_asset_id]
    del(assets[first_asset_id])
    return build_asset_list(assets) + [ asset ]

SQL_check_if_exists = """\
select "__source_unique_id", "__version"
from "{0}" where "__source_unique_id" in (\n\t\'{1}\'\n)
"""

SQL_get_asset_info = """\
select
	f.file_id as "file_id",
	f.filestore_id as "filestore_id",
	f.eas_id as "eas_id",
	f.eas_root_id "eas_root_id",
	fs.original_filename as "original_filename",
        fs.url is not null as "has_url",
        fs.filename is not null as "has_filename",
        fs.data is not null as "has_data",
        fs.url as url,
        fs.filename as filename
from file f
join filestore fs on (
	f.filestore_id = fs.filestore_id and
	f.source_name = ? and
	f.source_table_name = ? and
	f.source_column_name = ? and
	f.source_unique_id = ?
)
"""
