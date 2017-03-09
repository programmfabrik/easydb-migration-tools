#!/usr/bin/python3
import logging
import logging.config
import sys
import sqlite3


import easydb.migration.transform.job
import easydb.migration.transform.prepare
from easydb.migration.transform.extract import AssetColumn


#execution: ./transform eadb-url source-directory destination-directory --login LOGIN --password PASSWORD || requires source-db named "source.db" in source-directory
# setup
job = easydb.migration.transform.job.TransformJob.create_job('INSTANZNAME', easydb.migration.transform.prepare.CreatePolicy.IfNotExists)#creates transform-job named "INSTANZNAME" (change accordingly)


#logger-setup, doesnt have to be changed
standard_formatter = logging.Formatter('%(asctime)s %(levelname)s [%(name)s] %(message)s', '%Y.%m.%d %H:%M:%S')
user_formatter = logging.Formatter('%(message)s')
root_logger = logging.getLogger()
root_logger.setLevel('DEBUG')
user_logger = logging.getLogger('user')
user_logger.setLevel('DEBUG')

console_log = logging.StreamHandler()
console_log.setLevel(logging.DEBUG)
console_log.setFormatter(standard_formatter)
root_logger.addHandler(console_log)

migration_log = logging.FileHandler('{}/migration.log'.format(job.destination_dir))
migration_log.setLevel(logging.DEBUG)
migration_log.setFormatter(standard_formatter)
root_logger.addHandler(migration_log)

user_log = logging.FileHandler('{}/user.log'.format(job.destination_dir))
user_log.setLevel(logging.DEBUG)
user_log.setFormatter(user_formatter)
user_logger.addHandler(user_log)

logging.getLogger('easydb.server').setLevel('WARN')
logging.getLogger('requests').setLevel('WARN')
logging.getLogger('easydb.repository').setLevel('WARN')
logging.getLogger('easydb.migration.transform.source').setLevel('WARN')
logging.getLogger('easydb.migration.transform.prepare').setLevel('INFO')
#logging.getLogger('easydb.migration.transform.extract').setLevel('INFO')

#create destination.db
job.prepare()
# transform

def final_touch(tables):
    source_conn = sqlite3.connect(job.source.filename)
    source_c = source_conn.cursor()
    destination_conn = sqlite3.connect(job.destination.filename)
    destination_c = destination_conn.cursor()
    
    destination_c.execute('DELETE FROM "easydb.ez_user" WHERE login="root"')#Delete root-user, to prevent conflicting unique_user constraint (root is default system-user)
    destination_c.execute('INSERT INTO "easydb.ez_pool" ("__source_unique_id", "name:de-DE") VALUES ("STANDARD", "STANDARD_FALLBACK")')#create FALLBACK-pool for any records that have no pool 
    
    for table in tables:

        if table['has_parent']:
            req = 'SELECT fk_father_id, id FROM "' + table["table_from"] +'"'#get parent-ids from source
            for row in source_c.execute(req):
                if row[0]!=None:
                    write = 'UPDATE "{0}" SET __parent_id = '.format(table["table_to"]) + str(row[0]) + ' WHERE __source_unique_id = ' + str(row[1])#set parent-id for lists with hierarchical-ordering
                else:
                    write = 'UPDATE "{0}" SET __parent_id = NULL'.format(table["table_to"]) + ' WHERE __source_unique_id = ' + str(row[1])#set no parent-id
                destination_c.execute(write)
        if table['has_pool']:
            destination_c.execute('UPDATE "{0}" SET __pool_id ="STANDARD" WHERE __pool_id = NULL'.format(table["table_to"]))#set pool-id for records that are supposed to be organized in pool, but have no pool assigned 
    destination_conn.commit()

tables=[]#list of all tables, a transformation for each table must be appended in the dictionary stile below

tables.append(
    {
        'table_from': 'beispiel-instanz.schema.beispiel',#table in source
        'table_to': 'easydb.beispiel',#table in destination
        'sql': 
        """\
        SELECT
            id as __source_unique_id,
            name,
            name as "displayname:de-DE"
        FROM "instanz.schema.table_from"
        """,#sql query (hard to automatize, because of varying join, etc.), all fields are examples, must replace those
        'has_parent': False,#True if Object is part of a List with hierarchical ordering
        'has_pool': False,#True if records of this table are orgranized in pools
        'has_asset': False#True if record has a file attached to it
    }
)

for table in tables:
    
    if table[has_asset]:#Write records with files attachec
        asset_columns = [AssetColumn('beispiel-instanz', 'schmea.assets', 'bild', 'assets', 'bild', ['url'])]
        job.extract_sql(table['sql'], table['table_to'], asset_columns=asset_columns)
    
    else:#write assets with no file
        job.extract_sql(table['sql'], table['table_to'])

final_touch(tables)
job.log_times()