#!/usr/bin/python
# coding=utf8

import sys
import argparse
import os
import extract
import requests
import json

argparser = argparse.ArgumentParser(description='Extraction')
argparser.add_argument('easydb_url',            help='Base URL of the easydb4')
argparser.add_argument('user',                  help='easydb-user with admin rights')
argparser.add_argument('password',              help='User password')
argparser.add_argument('asset_version',         help='Asset Version, default = original')
argparser.add_argument('storage_method',        help='"URL" for later retrival or "File" for extracting assets along with data, default = URL')
argparser.add_argument('-i', '--init', action='store_true', help='If set existing files will be purged')
argparser.add_argument('-t', '--tables', nargs='*', help='destination directory')

args = argparser.parse_args()

req_url='{}/ezadmin/dumpconfig'.format(args.easydb_url)
print(req_url)

ez_conf = requests.get(req_url, auth=(args.user,args.password)).json()

easydb = args.easydb_url.split('.')[0].split('//')[1]

pg_dsn = ez_conf['PDO_DATA_DSN'].split(':')[1].replace(';',' ')

sqlite_file = ez_conf['PDO_DESIGN_DSN'].split(':')[1]

eas_url = ez_conf['EAS_INTERNAL_URL']

eas_instance =  ez_conf['INSTANCE']

asset_version = args.asset_version

storage_method = args.storage_method

extract.__pg_init()
extract.__sqlite_init()
extract.prepare_source('source.db', init=True)
eadb_link_index = """
CREATE INDEX "%TABLE_NAME_IN_SOURCE%_idx"
ON "%TABLE_NAME_IN_SOURCE%" (from_table_id, to_table_id, from_id, to_id);
"""
extract.sqlite_to_source(
    name=easydb,
    filename=sqlite_file
)

extract.pg_to_source(
    name=easydb,
    schema_name='public',
    dsn=pg_dsn,
    include_tables_exclusive=False,
    include_tables = {
    '{}.eadb_links'.format('public'): {
        'onCreate': eadb_link_index
        }
    },
    exclude_tables = [
        '{}.eadb_changelog'.format('public'),
        '{}.eadb_table_sql_changelog'.format('public'),
        '{}.eadb_rights'.format('public')
        ]
)

extract.eas_to_source(
    name=easydb,
    url=eas_url,
    instance=eas-instance,
    eas_versions = {
        asset_version: [storage_method]
        }
)
extract.__commit_source()
