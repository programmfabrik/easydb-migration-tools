#!/usr/bin/python
# coding=utf8

import sys
import argparse
import os
import easydb.migration.extract
#execution: ./extraction.py source-directory
###############################################################################

##INSTANZSPEZIFISCHE VARIABLEN
##VOR AUSFÜHRUNG SETZEN!

schema= "public"                                #meistens 'public' Bei mehreren Schemata manuell für jeden Tabellen Eintrag festlegen
instanz= None                                   #Instanzname in Postgres z.B. lette-verein, easy5-annegret o.ä.
eas-instanz
###############################################################################
if schema is None or instanz is None:
    print('Instanzspezifische Variablen festlegen')
    sys.exit(0)


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
    name=instanz,
    filename='/opt/easydb/4.0/sql/sqlite/{}.sqlite'.format(instanz) #e.g. /var/lib/sqlite/instanz-name.sqlite or /opt/easydb/4.0/sql/sqlite/instanz-name.sqlite
)
#Extracts Record-Data from PostgreSQL-DB
easydb.migration.extract.pg_to_source(
    name=instanz,
    schema_name=schema,#usually public is fine for every instance
    dsn='dbname={} port=5440 user=postgres'.format(instanz),#often port is 5432
    include_tables_exclusive=False,
    include_tables = {
        '{}.eadb_links'.format(schema): {
            'onCreate': eadb_link_index
        }
    },
    exclude_tables = [
        '{}.eadb_changelog'.format(schema),
        '{}.eadb_table_sql_changelog'.format(schema),
        '{}.eadb_rights'.format(schema)
    ]
)
#Extracts File-Links from EAS-Server
easydb.migration.extract.eas_to_source(
    name=instanz,
    url="localhost/eas",#ususallay instanz-url.domain/eas or localhost/eas if executed on the same machine
    instance=eas-instanz,
    eas_versions = {
       'original': ['url']#can be changed to store all asset files in data blob, but for migration URLs are working fine
    }
)

easydb.migration.extract.__commit_source()
