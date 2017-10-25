'''
* easydb-migration-tools - easydb Migration Tools
 * Copyright (c) 2013 - 2016 Programmfabrik GmbH
 * MIT Licence
 * https://github.com/programmfabrik/coffeescript-ui, http://www.coffeescript-ui.org
'''

__all__ = [
    'load',
    'CustomNestedLoader'
]

import logging
import json
import re
import requests
import os
import hashlib

from easydb.repository.base import *
from easydb.server.datamodel import *
from easydb.server.object import *
from easydb.server.group import *
from easydb.server.user import *
from easydb.server.collection import *
from easydb.server.collection_object import *
from easydb.tool.sql import sql_list
from easydb.server.pool import *
from easydb.tool.json import *
from easydb.tool.batch import *

# public

def load(
    source,
    destination,
    ezapi,
    eas_url,
    eas_instance,
    objecttypes=None,
    custom_nested_loaders={},
    batch_size=1000,
    stop_on_error=True,
    search_assets=True,
    verify_ssl=True,
    tmp_asset_file='/tmp/easy5-migration-asset'):
    global logger
    logger = logging.getLogger('easydb.migration.load.loader')
    collection_objects=False
    if objecttypes is None:
        raise Exception('Destination.load: objecttypes=None not yet implemented')
    manage_source = not source.is_open()
    if manage_source:
        source.open()
    ez_schema = destination.get_ez_schema()
    for objecttype in objecttypes:
        if objecttype == 'ez_pool':
            load_pools(source, destination, ezapi, batch_size)
        elif objecttype == 'ez_group':
            load_groups(source, destination, ezapi, batch_size)
        elif objecttype == 'ez_user':
            load_users(source, destination, ezapi, batch_size)
        elif objecttype == 'ez_tag':
            load_tags(source, destination, ezapi, batch_size)
        elif objecttype == 'ez_collection':
            load_collections(source, destination, ezapi, batch_size)
        elif objecttype == 'ez_collection__objects':
            load_collection_objects(source, destination, ezapi, batch_size)
        else:
            cnl = {}
            if objecttype in custom_nested_loaders:
                cnl = custom_nested_loaders[objecttype]
            load_objects(source, destination, ezapi, eas_url, eas_instance, batch_size, ez_schema, objecttype, tmp_asset_file, stop_on_error, search_assets, verify_ssl, cnl)

    for objecttype in objecttypes:
        if objecttype not in ('ez_pool','ez_group', 'ez_user', 'ez_tag', 'ez_collection', 'ez_collection_objects'):
            load_links(source, destination, ezapi, eas_url, eas_instance, batch_size, ez_schema, objecttype, tmp_asset_file, stop_on_error, search_assets, verify_ssl)
    if manage_source:
        source.close()


# - pools

def load_pools(
    source,
    destination,
    ezapi,
    batch_size):

    logger.info('load pools')
    loop = True
    while(loop):
        loop = False
        sql = build_get_pools_statement(destination.get_schema_languages())
        db = destination.get_db()
        db.open()
        rows = db.execute(sql)
        job = BatchedJob(BatchMode.List, batch_size, load_pools_batch, ezapi, db)
        for row in rows:
            loop = True
            logger.debug('load pool row: {0}'.format(row))
            job.add(Pool.from_row(row))
        job.finish()
        del(rows)
        db.close()

def load_pools_batch(batch, ezapi, db):
    ezapi.create_pools(batch)
    for pool in batch:
        db.execute(sql_update_pool_easydb_id, pool.id, pool.source_id)

def build_get_pools_statement(languages):
    s = ''
    for column_name in ['name', 'description']:
        for language in languages:
            s += ',\n\tc."{0}:{1}"'.format(column_name, language)
    return sql_get_pools.format(s)
# -  Collections

def load_collections(
    source,
    destination,
    ezapi,
    batch_size):

    logger.info('load collection')

    db = destination.get_db()
    db.open()
    users=db.execute('SELECT login, __easydb_id FROM "easydb.ez_user"').get_rows()
    db.close()
    logger.info('SET COLLECTION OWNERS IN DESTINATION')

    for user in users:
        db.open()
        owner='user_'+user['login']
        sql='UPDATE "easydb.ez_collection" SET "__owner_id" = {}, __owner="{}" WHERE "__owner"="{}"'.format(user['__easydb_id'],user['login'],owner)
        db.execute(sql)
        db.close()

    collections=ezapi.get('collection/list')
    root_collection_id=collections[0]['collection']['_id']

    collections=ezapi.get('collection/list/{}'.format(root_collection_id))

    for collection in collections:
        db.open()
        collection_id=collection['collection']['_id']
        if collection['_owner']['_basetype'] == 'user':
            collection_owner_id=collection['_owner']['user']['_id']
        else:
            continue
        sql='UPDATE "easydb.ez_collection" SET "__user_collection_id" = {} WHERE "__owner_id"={} AND "__parent_id" is NULL'.format(collection_id, collection_owner_id)
        db.execute(sql)
        db.close()

    logger.info('UPLOAD COLLECTIONS')

    loop = True
    while(loop):
        db.open()
        loop = False
        sql = build_get_collections_statement(destination.get_schema_languages())
        rows = db.execute(sql)
        job = BatchedJob(BatchMode.List, batch_size, load_collections_batch, ezapi, db)
        for row in rows:
            loop = True
            logger.info('load collection row: {0}'.format(row['displayname:de-DE']))
            job.add(Collection.from_row(row))
        job.finish()
        del(rows)
        db.close()

def load_collections_batch(batch, ezapi, db):
    ezapi.create_collections(batch)
    for collection in batch:
        db.execute(sql_update_collection_easydb_id, collection.id, collection.source_id)
        db.execute(sql_update_collection_easydb_id_objects, collection.id, collection.source_id)

def build_get_collections_statement(languages):
    s = ''
    for column_name in ['displayname', 'description']:
        for language in languages:
            s += ',\n\tc."{0}:{1}"'.format(column_name, language)

    return sql_get_collections.format(s)

##ES müssen erst alle easydb-Objekte erstellt werden, bevor die Collections/Mappen gefüllt werden können

def load_collection_objects(
    source,
    destination,
    ezapi,
    batch_size):
    db = destination.get_db()
    db.open()

    tables = db.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables_rows=tables.get_rows()
    del(tables)
    db.close()

    for table in tables_rows:
        logger.info('Updating collection_objects GOID for type {}'.format(table['name']))
        if table['name'] == "easydb.ez_collection__objects":
            continue
        db.open()
        columns = db.execute('PRAGMA table_info("{}")'.format(table['name']))
        columns_rows=columns.get_rows()
        del(columns)
        db.close()
        for column in columns_rows:
            name = column['name']
            if name == "collection_id":
                db.open()
                rows = db.execute('SELECT __source_unique_id, __easydb_goid, collection_id FROM "{}"'.format(table['name']))
                rows_rows=rows.get_rows()
                del(rows)
                db.close()
                for row in rows_rows:
                    if row['collection_id'] is not None:
                        db.open()
                        db.execute('UPDATE "easydb.ez_collection__objects" SET object_goid = "{}" WHERE object_id = "{}"'.format(row['__easydb_goid'], row['__source_unique_id']))
                        db.close()
    logger.info('LOAD COLLECTION OBJECTS')
    loop = True
    while(loop):
        print("BIS ZUR WHILE")
        loop = False
        db.open()
        sql = 'SELECT * FROM "easydb.ez_collection__objects" co JOIN  "easydb.ez_collection" c on (co.collection_id = c.__source_unique_id) where co."uploaded" is null'
        rows = db.execute(sql).get_rows()
        db.close()
        job = BatchedJob(BatchMode.List, batch_size, load_collection_objects_batch, ezapi, db)
        for row in rows:
            loop = True
            job.add(Collection_Object.from_row(row))
        job.finish()

    logger.debug("ADDING FONTEND_PROPS FOR PRESENTATIONS")
    db.open()
    presentations = db.execute('SELECT * FROM "easydb.ez_collection" WHERE __type = "presentation"')
    presentations_rows=presentations.get_rows()
    del(presentations)
    db.close()
    for presentation in presentations_rows:
        db.open()
        slides = db.execute('SELECT object_goid, position FROM "easydb.ez_collection__objects" WHERE collection_id={} ORDER BY position ASC'.format(presentation["__easydb_id"]))
        slides_rows=slides.get_rows()
        del(slides)
        db.close()
        name= presentation["displayname:de-DE"]
        slides_a=[]
        for slide in slides_rows:
            goid = slide["object_goid"]
            slide_d = {}
            slide_d["type"]="one"
            slide_d["center"]={"_global_object_id": goid}
            slides_a.append(slide_d)
        presentation_d={"slide_idx": 1, "slides": slides_a, "settings": {"show_info": "no-info"}}
        frontend_props = {"presentation": presentation_d}
        collection_d = {"_version": 2, "webfrontend_props": frontend_props}
        diction = {"collection": collection_d}
        call="collection/{}".format(presentation["__easydb_id"])
        response_object = ezapi.post(call, diction)


def load_collection_objects_batch(batch, ezapi, db):
    ezapi.create_collection_objects(batch)
    for collection_object in batch:
        db.open()
        db.execute(sql_update_collection_object_easydb_id, collection_object.uploaded, collection_object.source_id)
        db.close()
# - Groups#

def load_groups(
    source,
    destination,
    ezapi,
    batch_size):

    logger.info('load groups')
    sql = build_get_groups_statement(destination.get_schema_languages())
    db = destination.get_db()
    db.open()
    rows = db.execute(sql)
    job = BatchedJob(BatchMode.List, batch_size, load_groups_batch, ezapi, db)
    for row in rows:
        logger.debug('load group row: {0}'.format(row))
        job.add(Group.from_row(row))
    job.finish()
    del(rows)
    db.close()

def load_groups_batch(batch, ezapi, db):
    ezapi.create_groups(batch)
    for group in batch:
        db.execute(sql_update_group_easydb_id, group.id, group.source_id)

def build_get_groups_statement(languages):
    s = ''
    for language in languages:
        s += ',\n\t"displayname:{0}"'.format(language)
    return sql_get_groups.format(s)

def load_users(
    source,
    destination,
    ezapi,
    batch_size):

    logger.info('load users')
    db = destination.get_db()
    db.open()
    rows = db.execute(sql_get_users)
    job = BatchedJob(BatchMode.List, batch_size, load_users_batch, ezapi, db)
    for row in rows:
        logger.debug('load user row: {0}'.format(row))
        group_rows = db.execute(sql_get_user_groups.format(row['__source_unique_id']))
        job.add(User.from_rows(row, group_rows))
        del group_rows
    job.finish()
    del(rows)
    db.close()

def load_users_batch(batch, ezapi, db):
    ezapi.create_users(batch)
    for user in batch:
        db.execute(sql_update_user_easydb_id, user.id, user.source_id)

def load_tags(
    source,
    destination,
    ezapi,
    batch_size):

    logger.info('load tags')
    sql = build_get_tag_groups_statement(destination.get_schema_languages())
    db = destination.get_db()
    db.open()
    rows = db.execute(sql)
    js = []
    tag_groups = []
    for row in rows:
        tag_group = {
            'taggroup': {
                'type': row['type'],
                'displayname': {}
            },
            '_tags': []
        }
        for lan in destination.get_schema_languages():
            tag_group['taggroup']['displayname'][lan] = row['displayname:{0}'.format(lan)]
        group = row['__source_unique_id']
        sql = build_get_tags_statement(group, destination.get_schema_languages())
        subrows = db.execute(sql)
        tags = []
        for subrow in subrows:
            tag = {
                'tag': {
                    'type': subrow['type'],
                    'displaytype': subrow['displaytype'],
                    'displayname': {},
                    'enabled': True
                }
            }
            for lan in destination.get_schema_languages():
                tag['tag']['displayname'][lan] = subrow['displayname:{0}'.format(lan)]
            tag_group['_tags'].append(tag)
            tags.append(subrow['__source_unique_id'])
        del subrows
        js.append(tag_group)
        tag_groups.append((group, tags))
    del rows
    if len(js) > 0:
        logger.debug('PUSH tags:\n{0}'.format(json.dumps(js, indent=4)))
        response_objects = ezapi.post('tags', js)
        if len(response_objects) != len(tag_groups):
            raise Exception('response tag groups are different from pushed tag groups')
        for i in range(len(tag_groups)):
            tag_group_id, tags = tag_groups[i]
            db.execute(sql_update_tag_group_easydb_id, extract_from_json(response_objects[i], 'taggroup._id'), tag_group_id)
            if len(response_objects[i]['_tags']) != len(tags):
                raise Exception('response tags are different from pushed tags')
            for j in range(len(tags)):
                db.execute(sql_update_tag_easydb_id, extract_from_json(response_objects[i]['_tags'][j], 'tag._id'), tags[j])
    db.close()

def build_get_tag_groups_statement(languages):
    s = ''
    for language in languages:
        s += ',\n\t"displayname:{0}"'.format(language)
    return sql_get_tag_groups.format(s)

def build_get_tags_statement(group, languages):
    s = ''
    for language in languages:
        s += ',\n\t"displayname:{0}"'.format(language)
    return sql_get_tags.format(group, s)

# - user objects

def load_objects(
    source,
    destination,
    ezapi,
    eas_url,
    eas_instance,
    batch_size,
    ez_schema,
    objecttype,
    tmp_asset_file,
    stop_on_error,
    search_assets,
    verify_ssl,
    custom_nested_loaders):

    objecttype = ez_schema.objecttypes[objecttype]
    logger.info('[load-objects] begin - objecttype "{0}"'.format(objecttype.name))
    loader = Loader(source, destination, ez_schema, ezapi, eas_url, eas_instance, objecttype, tmp_asset_file, stop_on_error, search_assets, verify_ssl)
    loader.custom_nested_loaders = custom_nested_loaders
    loader.preload()
    loader.prepare_query()
    loop = True
    while(loop):
        loop = False
        objects = []
        db = destination.get_db()
        db.open()
        logger.info('[load-objects] get next')
        rows = get_next_objects(db, objecttype)
        for row in rows:
            objects.append(row['__source_unique_id'])
        del rows
        db.close()
        logger.info('[load-objects] start batched job')
        job = BatchedJob(BatchMode.List, batch_size, loader.load)
        for object_id in objects:
            loop = True
            job.add(object_id)
        job.finish()
    logger.info('[load-objects] end')

def get_next_objects(db, objecttype):
    if objecttype.is_hierarchical:
        sql = sql_get_next_objects_hierarchical
    else:
        sql = sql_get_next_objects
    return db.execute(sql.format(objecttype.name))

def load_links(
    source, 
    destination, 
    ezapi, 
    eas_url, 
    eas_instance, 
    batch_size, 
    ez_schema, 
    objecttype, 
    tmp_asset_file, 
    stop_on_error, 
    search_assets, 
    verify_ssl):
    objecttype = ez_schema.objecttypes[objecttype]
    has_links=False
    for column_def in self.objecttype.columns.values():
            if column_def.kind == 'link':
                has_links=True
            elif column_def.kind == 'column' and column_def.column_type == 'link':
                has_links=True
    if not has_links:
        logger.info('[{0}] Skipping, has no links'.format(objecttype.name))
        return
    logger.info('[{0}] Updating Links'.format(objecttype.name))
    loader = Loader(source, destination, ez_schema, ezapi, eas_url, eas_instance, objecttype, tmp_asset_file, stop_on_error, search_assets, verify_ssl)
    number_of_objects=easydb_api.post("search?pretty=0",js=search_js)["count"]
    offset=0
    db = destination.get_db()
    while((offset+batch_size)<number_of_objects):
        logger.info('[{0}] Fetching Objects'.format(objecttype.name))
        objects_in=easydb_api.get("db/bilder/_all_fields/list?limit={0}&offset={1}&format=short".format(batch_size,offset))
        offset+=batch_size
        objects_out=[]
        for object in objects_in:
            loop = True
            objects_out.append(loader.load_linked(object,row))
            if len(objects_out)>=batch_size or len(objects_out)==len(objects_in):
                logger.info('[{0}] updating batch of {1}'.format(objecttype.name,batch_size))
                response=ezapi.post("db/{}".format(objecttype.name), objects_out)
                if len(response) != len(objects_out):
                    raise Exception('response objects are different from pushed objects')
                db.open()
                for obj in response:
                    query='UPDATE "easydb.{}" SET __updated = "TRUE" WHERE __easydb_goid="{}"'.format(objecttype.name, obj["_global_object_id"])
                    db.execute(query)
                db.close()
                objects_out=[]     
    logger.info('[update-objects] end')

class Loader(object):

    def __init__(self, source, destination, ez_schema, ezapi, eas_url, eas_instance, objecttype, tmp_asset_file, stop_on_error, search_assets, verify_ssl = True, uplink_id=None):
        self.source = source
        self.destination = destination
        self.ez_schema = ez_schema
        self.objecttype = objecttype
        self.ezapi = ezapi
        self.eas_url = eas_url
        self.eas_instance = eas_instance
        self.uplink_id = uplink_id
        self.table_def = destination.get_table_for_objecttype(objecttype.name)
        self.tmp_asset_file = tmp_asset_file
        self.stop_on_error = stop_on_error
        self.logger = logging.getLogger('easydb.etl.load')
        self.custom_nested_loaders = {}
        self.search_assets = search_assets
        self.verify_ssl = verify_ssl

    def preload(self):
        for objecttype, custom_loader in self.custom_nested_loaders.items():
            logger.info('[preload] {0}'.format(objecttype))
            custom_loader.preload(self)

    def prepare_query(self):
        self.tables = LoaderTables()
        self.columns = LoaderColumns()
        self.selects = []
        self.joins = []

        self.tables.add(self.table_def.name)
        self.columns.add(self.tables.main(), '__source_unique_id', '_id')
        if self.uplink_id is None:
            self.columns.add(self.tables.main(), '__comment', '__comment')
        else:
            self.columns.add(self.tables.main(), '__uplink_id', '__uplink_id')
        if self.objecttype.pool_link:
            self.prepare_query__pool()
        if self.objecttype.is_hierarchical:
            self.prepare_query__parent()
        for column_def in self.objecttype.columns.values():
            if column_def.kind == 'column':
                if column_def.column_type == 'link':
                    self.prepare_query__link(column_def)
                else:
                    if "l10n" in column_def.column_type:
                        languages =  self.destination.get_schema_languages()
                        for language in languages:
                            name=column_def.name+":"+language
                            self.columns.add(self.tables.main(), name, column_def.name, True)
                    elif column_def.column_type == 'eas':
                        continue
                    else:
                        self.columns.add(self.tables.main(), column_def.name, column_def.name)

    def prepare_query__pool(self):
        table = self.tables.add("easydb.ez_pool")
        self.columns.add(table, '__easydb_id', '__pool_id')
        self.joins.append('left join {0} on {1}."__source_unique_id" = t0."__pool_id"'.format(table.name_alias, table.alias))

    def prepare_query__parent(self):
        table = self.tables.add(self.table_def.name)
        self.columns.add(table, '__easydb_id', '__parent_id')
        self.joins.append('left join {0} on {1}."__source_unique_id" = t0."__parent_id"'.format(table.name_alias, table.alias))

    def prepare_query__link(self, column_def):
        for constraint_def in self.table_def.constraints:
            if isinstance(constraint_def, ForeignKeyConstraintDefinition):
                if len(constraint_def.own_columns) != 1 or len(constraint_def.ref_columns) != 1:
                    raise Exception('fks with more than one column not yet supported')
                field_name = constraint_def.own_columns[0]
                if column_def.name == field_name:
                    rt = self.tables.add(constraint_def.ref_table_name)
                    rc = quote_name(constraint_def.ref_columns[0])
                    oc = quote_name(field_name)
                    self.columns.add(rt, '__easydb_id', field_name)
                    self.joins.append('left join {0} on {1}.{2} = t0.{3}'.format(rt.name_alias, rt.alias, rc, oc))
                    return
        raise Exception('could not find a foreign key for link {0}'.format(name))

    def load(self, object_source_ids):
        logger.info('[{0}] begin ({1} objects)'.format(self.objecttype.name, len(object_source_ids)))
        db = self.destination.get_db()
        db.open()
        logger.info('[{0}] execute query'.format(self.objecttype.name))
        rows = self.execute_query(db, object_source_ids)
        current_source_id = None
        current_rows = []
        objects = []
        logger.info('[{0}] process rows'.format(self.objecttype.name))
        for row in rows:
            this_source_id = row['f0']
            if current_source_id is None:
                current_source_id = this_source_id
            elif this_source_id != current_source_id:
                objects.append(self.build_object(db, current_rows))
                current_rows = []
            current_rows.append(row)
            current_source_id = this_source_id
        del rows
        if len(current_rows) > 0:
            objects.append(self.build_object(db, current_rows))
        db.close()
        logger.info('[{0}] push objects'.format(self.objecttype.name))
        try:
            db = self.destination.get_db()
            db.open()
            self.push_objects(db, objects)
            db.close()
        except Exception as e:
            db.close()
            if self.stop_on_error:
                raise e
            else:
                self.logger.error('Could not load batch (do not stop on error):\n{0}'.format(str(e)))
        logger.info('[{0}] end'.format(self.objecttype.name))

    def load_linked(self, obj, row):
        db = self.destination.get_db()
        for column_def in self.objecttype.columns.values():
            if column_def.kind == 'link':
                for ot_name, ot in self.ez_schema.objecttypes.items():
                    if ot_name == column_def.other_table:
                        other_ot = ot
                        break
                else:
                    raise Exception('table {0} not found'.format(column_def.other_table))
                
                linked_objects=[]
                db.open()
                other_rows=db.execute('SELECT * FROM "easydb.{}" WHERE __uplink_id={}'.format(other_ot.name,row['__source_unique_id'])).get_rows()
                db.close()
                for other_row in other_rows:
                    linked_object={}
                    for other_column_def in other_ot.columns.values():
                        if other_column_def.column_type=="eas":
                            asset_info=self._load_assets(db, other_row['__source_unqiue_id'],other_column_def, other_ot)
                            value=[]
                            for eas_id, preferred in asset_info:
                                value.append({'_id': eas_id, 'preferred': preferred})
                            linked_object[column]=value
                            continue
                        for column in other_row.keys():
                            if column == other_column_def.name:
                                for const in other_ot.constraints:
                                    if column in const.own_columns:
                                        ref_table=const.ref_table_name
                                        sql='SELECT * FROM "{}" WHERE __source_unique_id={}'.format(ref_table,other_row[column])
                                        db.open()
                                        linked_row=db.execute(sql).get_rows()
                                        db.close()
                                        linked_object[column]={}
                                        linked_object[column]["_objecttype"]=const.ref_table_name[7:]
                                        linked_object[column]["_mask"]=const.ref_table_name[7:]+"__all_fields"
                                        linked_object[column][const.ref_table_name[7:]]={}
                                        linked_object[column][const.ref_table_name[7:]]["_id"]=int(linked_row[0]["__easydb_id"])
                                    else:
                                        linked_object[column]=other_row[column]
                                    break
                                break
                    linked_objects.append(linked_object)

                if '_nested:{}'.format(other_ot.name) in obj[0][self.objecttype.name].keys():
                    obj[0][self.objecttype.name]['_nested:{}'.format(other_ot.name)].extend(linked_objects)
                else:
                    obj[0][self.objecttype.name]['_nested:{}'.format(other_ot.name)] = linked_objects

            elif column_def.kind == 'column' and column_def.column_type == 'link':
                for const in self.objecttype.constraints:
                    if column_def.name in const.ref_columns:
                        ref_table=const.ref_table_name
                        linked_row=db.execute('SELECT * FROM "easydb.{}" WHERE __source_unique_id={}'.format(ref_table,row['__source_unique_id']))
                        obj[0][self.objecttype.name][column_def.name]=linked_row[0]["__easydb_id"]
                        break
                else:
                    obj[0][self.objecttype.name][column_def.name]=row[column_def.name]
        obj[0][self.objecttype.name]['_version']+=1
        return obj[0]

    def execute_query(self, db, object_source_ids):
        args = []
        sql_columns = self.columns.select()
        sql_main_table = self.tables.main().name_alias
        sql_joins = '\n'.join(self.joins)
        sql_order = 'order by t0."__source_unique_id"'
        if self.uplink_id is not None:
            sql_where += ' and t0."__uplink_id" = ?'
            sql_order = 'order by t0."__uplink_id"'
            args.append(self.uplink_id)
        sql = sql_load_objects.format(sql_columns, sql_main_table, sql_joins, sql_list(object_source_ids))
        return db.execute(sql, *args)

    def build_object(self, db, rows):
        logger.debug('[{0}] build-object begin'.format(self.objecttype.name))
        o = Object(self.objecttype)
        o.source_id = rows[0]['f0']
        current_col = 0
        if not self.uplink_id:
            current_col += 1
            comment = rows[0]['f{0}'.format(current_col)]
            if comment is not None:
                o._comment = comment
        if self.objecttype.pool_link:
            current_col += 1
            o._pool_id = rows[0]['f{0}'.format(current_col)]
        if self.objecttype.is_hierarchical:
            current_col += 1
            o._parent_id = rows[0]['f{0}'.format(current_col)]
        for column_def in self.objecttype.columns.values():
            if column_def.kind == 'column':
                if column_def.column_type == 'eas' or column_def.column_type == 'link':
                    continue
                elif 'l10n' in column_def.column_type:
                    l = self.destination.get_schema_languages()
                    value={}
                    for language in l:
                        name=column_def.name+":"+language
                        value[language] = rows[0][self.columns.get_column(name).alias]
                else:
                    value = rows[0][self.columns.get_column(column_def.name).alias]

            elif column_def.kind == 'link':
                continue
            o.fields[column_def.name] = value
            
        if self.objecttype.has_tags:
            sql = 'select b.__easydb_id as id from "tag.{0}" a join "easydb.ez_tag" b on (a.tag_id = b.__source_unique_id) where a.object_id = ? and b.__easydb_id is not null'.format(self.objecttype.name)
            rows = db.execute(sql, o.source_id)
            for row in rows:
                o._tags.append(row['id'])
            del rows
        logger.debug('[{0}] build-object end'.format(self.objecttype.name))
        return o

    def push_objects(self, db, objects):
        logger.info('[{0}] push begin ({1} objects)'.format(self.objecttype.name, len(objects)))
        check_sql = 'select "__easydb_id" from "easydb.{0}" where "__source_unique_id" = ?'.format(self.objecttype.name)
        objects_to_push = []
        for o in objects:
            rows = db.execute(check_sql, o.source_id)
            row = rows.next()
            if row['__easydb_id'] is None:
                objects_to_push.append(o)
            else:
                self.logger.debug('skipping object {0}:{1} because it was already pushed'.format(self.objecttype.name, o.source_id))
        if len(objects_to_push) == 0:
            return
        for column_def in self.objecttype.columns.values():
            if column_def.kind == 'column' and column_def.column_type == 'eas':
                for o in objects:
                    asset_info = self._load_assets(db, o.source_id, column_def, self.objecttype)
                    preferred_found = False
                    value = []
                    for eas_id, preferred in asset_info:
                        value.append({'_id': eas_id, 'preferred': preferred})
                        o.fields[column_def.name] = value
        logger.info('[{0}] create objects'.format(self.objecttype.name))
        self.ezapi.create_objects(objects_to_push)
        logger.info('[{0}] update destination'.format(self.objecttype.name))
        update_sql = 'update "easydb.{0}" set "__easydb_id" = ?, "__easydb_goid" = ? where "__source_unique_id" = ?'.format(self.objecttype.name)
        for o in objects:
            rows = db.execute(update_sql, o.id, o.global_object_id, o.source_id)
            if rows.rowcount != 1:
                raise Exception('could not update easydb id')
        logger.info('[{0}] push end'.format(self.objecttype.name))

    def _load_assets(self, db, object_id, column_def, objecttype):
        eas_table = 'asset.{0}.{1}'.format(objecttype.name, column_def.name)
        sql = sql_load_assets.format(quote_name(eas_table))
        rows = db.execute(sql, object_id)
        asset_info = []
        preferred_found = False
        for row in rows:
            if row['__eas_id'] is None:
                filename = row['original_filename']
                if filename is None:
                    filename = row['__source_unique_id']
                source_type = row['source_type']
                asset_file = self.tmp_asset_file
                logger.info('load asset for {0}:{1}:{2} from {3}:{4}'.format(self.objecttype.name, column_def.name, object_id, source_type, row['source']))
                if source_type == 'data':
                    sql = 'select data from filestore where filestore.filestore_id = ?'
                    data_rows = self.source.execute(sql, row['source'])
                    if len(data_rows) == 0:
                        logger.error('asset not found in filestore')
                        continue
                    data_row = data_rows.next()
                    with open(self.tmp_asset_file, 'wb') as output_file:
                        output_file.write(data_row['data'])
                elif source_type == 'url':
                    r = requests.get(row['source'], stream=True, verify=self.verify_ssl)
                    if r.status_code != 200:
                        logger.error('failed to fetch asset from URL {0}'.format(row['source']))
                        continue
                    with open(self.tmp_asset_file, 'wb') as output_file:
                        for chunk in r.iter_content(1024):
                            output_file.write(chunk)
                else:
                    asset_file = row['source']
                    if not os.path.isabs(asset_file):
                        asset_file = os.path.join(self.source.asset_dir, asset_file)
                    if not os.path.isfile(asset_file):
                        logger.error('asset not found in filesystem: {0}'.format(asset_file))
                        continue
                eas_id = self._load_asset(filename, asset_file, db, eas_table)
                if eas_id is None:
                    logger.error('EAS PUT failed for {0}:{1}:{2} from {3}:{4}'.format(self.objecttype.name, column_def.name, object_id, source_type, row['source']))
                    continue
                sql = 'update {0} set "__eas_id" = ? where "__source_unique_id" = ?'.format(quote_name(eas_table))
                db.execute(sql, eas_id, row['__source_unique_id'])
            else:
                eas_id = row['__eas_id']
            if not preferred_found:
                preferred = row['preferred'] == 1
                preferred_found = preferred
            else:
                preferred = False
            asset_info.append((eas_id, preferred))
        return asset_info

    def _load_asset(self, filename, asset_file, db, eas_table):
        if self.search_assets:
            eas_ids = self._search_asset(asset_file)
            if len(eas_ids) > 0:
                existing_eas_ids = set()
                sql = 'select __eas_id from {0} where __eas_id in ({1})'.format(quote_name(eas_table), sql_list(eas_ids))
                rows = db.execute(sql)
                for row in rows:
                    existing_eas_ids.add(row['__eas_id'])
                del rows
                for eas_id in eas_ids:
                    if eas_id in existing_eas_ids:
                        logger.debug('eas_id {0} found, but already in use'.format(eas_id))
                    else:
                        return eas_id
        return self._put_asset(filename, asset_file)

    def _search_asset(self, asset_file):
        eas_file_unique_id = get_eas_file_unique_id(asset_file)
        if eas_file_unique_id is None:
            logger.debug('search asset failed: get_eas_file_unique_id returned None')
            return []
        url = '{0}/search/keyword'.format(self.eas_url)
        params = {
            'instance': self.eas_instance,
            'type.unique_id': eas_file_unique_id
        }
        r = requests.get(url, params=params, verify = self.verify_ssl)
        if r.status_code != 200:
            logger.error('search asset failed: eas returned {0}: {1}'.format(r.status_code, r.text))
            return []
        eas_response = json.loads(r.text)
        if len(eas_response) == 0:
            logger.debug('asset with unique_id {0} not found'.format(eas_file_unique_id))
            return []
        eas_ids = []
        for asset in eas_response:
            v = {}
            compare_json(asset, { 'id': '$eas_id' }, v)
            eas_id = v['eas_id']
            logger.debug('asset found in eas: {0}'.format(eas_id))
            if self.destination.consume_asset(int(eas_id)):
                eas_ids.append(eas_id)
            else:
                logger.warn('found asset is not valid: {0}'.format(eas_id))
        return eas_ids

    def _put_asset(self, filename, asset_file):
        for i in range(1,4):
            try:
                logger.debug('loading asset: attempt {0}: {1}'.format(i, filename))
                r = self.ezapi.post('eas/put', files={'files[]': ( filename, open(asset_file, 'rb'))})
                v = {}
                compare_json(r, [{ '_id': '$eas_id' }], v)
                eas_id = v['eas_id']
                logger.debug('asset loaded to eas: {0}'.format(eas_id))
                return eas_id
            except Exception as e:
                logger.error('loading asset "{0}" failed: {1}'.format(filename, e))
                return None
        return None

def get_eas_file_unique_id(filename):
    try:
        with open(filename, 'rb') as f:
            data = f.read(4096)
        datahash = hashlib.sha1(data).hexdigest()
        length = os.stat(filename).st_size
        return hashlib.sha1('{0}/{1}'.format(datahash, length).encode('ascii','ignore')).hexdigest()
    except IOError as e:
        return None

sql_get_pools = """\
select
	c.__source_unique_id,
    c._standard_masks,
	p.__easydb_id as __parent_id{0},
    c.shortname as shortname
from "easydb.ez_pool" c
left join "easydb.ez_pool" p on c."__parent_id" == p."__source_unique_id"
where
	c."__easydb_id" is null and
	(c."__parent_id" is null or p."__easydb_id" is not null)
"""

sql_get_collections = """\
select
	c.__source_unique_id,
    c.__owner_id,
    c.__owner,
    c.__user_collection_id,
    c.__type,
	p.__easydb_id as __parent_id{0}
from "easydb.ez_collection" c
left join "easydb.ez_collection" p on c."__parent_id" == p."__source_unique_id"
where
	c."__easydb_id" is null and
	(c."__parent_id" is null or p."__easydb_id" is not null)
"""

sql_update_collection_object_easydb_id="""\
update "easydb.ez_collection__objects"
    set "uploaded" = ? where "__source_unique_id" = ?
"""

sql_update_pool_easydb_id = """\
update "easydb.ez_pool"
set "__easydb_id" = ? where "__source_unique_id" = ?
"""

sql_update_collection_easydb_id = """\
update "easydb.ez_collection"
set "__easydb_id" = ? where "__source_unique_id" = ?
"""

sql_update_collection_easydb_id_objects = """\
update "easydb.ez_collection__objects"
set "collection_id" = ? where "collection_id" = ?
"""

sql_get_groups = """\
select
	__source_unique_id,
	comment{0}
from "easydb.ez_group"
where "__easydb_id" is null
"""

sql_update_group_easydb_id = """\
update "easydb.ez_group"
set "__easydb_id" = ? where "__source_unique_id" = ?
"""

sql_get_users = """\
select *
from "easydb.ez_user" u
where "__easydb_id" is null
"""

sql_get_user_groups = """\
select
        g.__easydb_id as group_id
from "easydb.ez_user__group" ug
join "easydb.ez_group" g on (ug.group_id = g.__source_unique_id)
where user_id = {0}
"""

sql_get_tag_groups = """\
select
	__source_unique_id,
        type{0}
from "easydb.ez_tag_group"
where __easydb_id is null
"""

sql_get_tags = """\
select
	__source_unique_id,
        type,
        displaytype{1}
from "easydb.ez_tag"
where "group" = '{0}'
"""

sql_update_tag_group_easydb_id = """\
update "easydb.ez_tag_group"
set "__easydb_id" = ? where "__source_unique_id" = ?
"""

sql_update_tag_easydb_id = """\
update "easydb.ez_tag"
set "__easydb_id" = ? where "__source_unique_id" = ?
"""

sql_update_pool_easydb_id = """\
update "easydb.ez_pool"
set "__easydb_id" = ? where "__source_unique_id" = ?
"""

sql_update_user_easydb_id = """\
update "easydb.ez_user"
set "__easydb_id" = ? where "__source_unique_id" = ?
"""
##Order testen
sql_get_next_objects = """\
select __source_unique_id
from "easydb.{0}"
where __easydb_id is null
"""

sql_get_next_objects_hierarchical = """\
select c.__source_unique_id
from "easydb.{0}" c
where __easydb_id is null
and not exists (
	select *
	from "easydb.{0}" p
	where c."__parent_id" = p."__source_unique_id" and p.__easydb_id is null
)
"""

sql_get_next_objects_nested = """\
select __source_unique_id
from "easydb.{0}"
where __uplink_id = ?
"""

sql_load_objects = """
select {0}
from {1}
{2}
where t0.__source_unique_id in ({3})
order by t0.__source_unique_id
"""

sql_load_assets = """
select __source_unique_id, source_type, source, __eas_id, preferred, original_filename
from {0}
where object_id = ?
"""

search_js={
   "offset": 0,
   "limit": 1,
   "generate_rights": False,
   "search": [
      {
         "bool": "must",
         "type": "in",
         "fields": ["_objecttype"],
         "in": [
            objecttype
         ]
      }
   ]
}

class LoaderTables(object):
    def __init__(self):
        self.tables = []
    def add(self, name):
        table = LoaderTable(name, len(self.tables))
        self.tables.append(table)
        return table
    def main(self):
        return self.tables[0] if len(self.tables) > 0 else None

class LoaderTable(object):
    def __init__(self, name, order):
        self.name = name
        self.order = order
        self.alias = 't{0}'.format(self.order)
        self.name_alias = '{0} {1}'.format(quote_name(self.name), self.alias)

class LoaderColumns(object):
    def __init__(self):
        self.columns = []
        self.columns_by_field = {}
    def add(self, table, name, field,l10n=False):
        column = LoaderColumn(table, name, field, len(self.columns),l10n)
        self.columns.append(column)
        if field not in self.columns_by_field:
            self.columns_by_field[field] = []
        self.columns_by_field[field].append(column)
        return column
    def select(self):
        return ', '.join(map(lambda column : column.name_alias, self.columns))
    def get_columns(self, field):
        if field in self.columns_by_field:
            return self.columns_by_field[field]
        else:
            return []
    def get_column(self, field):
        if ":" in field:
            l10n_field=field.split(":")
            columns = self.get_columns(l10n_field[0])
            for col in columns:
                if col.name.endswith(l10n_field[1]):
                    return col;
        else:
            columns = self.get_columns(field)
            if len(columns) != 1:
                raise Exception('none or more than one column found for field {0}'.format(field))
            return columns[0]

class LoaderColumn(object):
    def __init__(self, table, name, field, order, l10n=False):
        self.table = table
        self.name = name
        self.field = field
        self.order = order
        self.l10n = l10n
        self.alias = 'f{0}'.format(self.order)
        self.name_alias = '{0}.{1} as {2}'.format(self.table.alias, quote_name(self.name), self.alias)

class CustomNestedLoader(object):
    def preload(self, main_loader):
        pass
    def load(self, db, uplink_id):
        return []
