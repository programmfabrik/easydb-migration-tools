#!/usr/bin/python
# coding=utf8

import sys
import argparse
import os

import easydb.migration.extract

easydb.migration.extract.__pg_init()
easydb.migration.extract.__sqlite_init()

parser = argparse.ArgumentParser('Extract Instanz-Name')
parser.add_argument('source', help='source db file')
parser.add_argument('--init', dest='init', action='store_const', const=True, default=False, help='if source exists, force init')
args = parser.parse_args()

if args.init:
    init = True
else:
    init = not os.path.isfile(args.source)

easydb.migration.extract.prepare_source(args.source, init=init)

eadb_link_index = """
CREATE INDEX "%TABLE_NAME_IN_SOURCE%_idx"
          ON "%TABLE_NAME_IN_SOURCE%" (from_table_id, to_table_id, from_id, to_id);
"""
#uses paths and urls for local instances on galaxy, check for correct ones for each instance 
#set name to the same value in every function call!

#Extracts structural Inforamtion about source from sqlite-file 
easydb.migration.extract.sqlite_to_source(
    name='instanz-name',
    filename='path_to_sqlite_file' #e.g. /var/lib/sqlite/instanz-name.sqlite or /opt/easydb/4.0/sql/sqlite/instanz-name.sqlite
) 
#Extracts Record-Data from PostgreSQL-DB
easydb.migration.extract.pg_to_source(
    name='instanz-name',
    schema_name='public',#usually public is fine for every instance
    dsn='dbname=instanz-name port=5440 user=postgres',#often port is 5432
    include_tables_exclusive=False,
    include_tables = {
        'public.eadb_links': {
            'onCreate': eadb_link_index
        }
    },
    exclude_tables = [
        'public.eadb_changelog',
        'public.eadb_table_sql_changelog',
        'public.eadb_rights'
    ]
)
#Extracts File-Links from EAS-Server
easydb.migration.extract.eas_to_source(
    name='instanz-name',
    url="http://easdbdev.4.0.mad.pf-berlin.de/eas",#ususallay instanz-url.domain/eas or localhost/eas if executed on the same machine
    instance="lette-verein",
    eas_versions = {
       'original': ['url']#can be changed to store all asset files in data blob, but for migration URLs are working fine
    }
)

easydb.migration.extract.__commit_source()
