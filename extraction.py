#!/usr/bin/python
# coding=utf8

import sys
import argparse
import os
import easydb.migration.extract
import requests
import json
import logging
import logging.config

logging.basicConfig(level=logging.DEBUG)


argparser = argparse.ArgumentParser(description='Extraction')

subparsers=argparser.add_subparsers(help="Set Export or Migration Mode", dest='mode')

migration_parser=subparsers.add_parser('migration', help="Set Migration Mode to create Source for Migration")
migration_parser.add_argument('--auto_fetch', nargs=3,                  help='Fetch Server Information from URL , usage: "--auto_fetch URL login password" If set no other arguments have to be set')
migration_parser.add_argument('--name_in_source',                       help='Name preceding schema and table names in Source, e.g. if export -> export.public.table')
migration_parser.add_argument('--easydb_pg_dsn',                        help='DSN for easydb-PostgreSQL Server')
migration_parser.add_argument('--easydb_sqlite_file',                   help='Filename for easydb_SQLite Database')
migration_parser.add_argument('--easydb_eas_url',                       help='URL for easydb-EAS-Server')
migration_parser.add_argument('--easydb_eas_instance',                  help='Instance-Name on EAS-Server')
migration_parser.add_argument('--source', default='source.db',          help='Source name and directory (Default: ./source.db)')

export_parser=subparsers.add_parser('ez_export', help="Export Data from easyDB to sqlite or mySQL")
export_parser.add_argument('--pg_dsn',                                  help='DSN for PostgreSQL')
export_parser.add_argument('--pg_tables', nargs='*', default=[],        help='Select Tables for Export from postgresql')
export_parser.add_argument('--eas_url',                                 help='URL for EAS-Server')
export_parser.add_argument('--eas_instance',                            help='Instance-Name on EAS-Server')
export_parser.add_argument('--assets',  nargs='*', default=[],          help='Asset Version and Storage-Method, enter "version:method", e.g "original:url"')
export_parser.add_argument('-o', '--output', default='dump.db',         help='Sqlite-Dump name and directory (Default: ./dump.db)')
export_parser.add_argument('--mySQL', action='store_true',              help='If set, Source will be dumped to mySQL file')

import_parser=subparsers.add_parser('file_import', help="Add to Source from other files")
export_parser.add_argument('--sqlite', nargs='*', default=[],           help='Filename for SQLite Database')
export_parser.add_argument('--XML', nargs='*', default=[],              help='Filename for XML')
export_parser.add_argument('--CSV', nargs='*', default=[],              help='Filename for CSV')
export_parser.add_argument('-o', '--output', default='dump.db',         help='Sqlite-Dump name and directory (Default: ./dump.db)')
export_parser.add_argument('--mySQL', action='store_true',              help='If set, Source will be dumped to mySQL file in same directory and with the same filename')

argparser.add_argument('--init', action='store_true',     help='If set, existing files will be purged')

args = argparser.parse_args()
print(args)
if args.mode=="migration":

    if args.auto_fetch is not None:
        req_url='{}/ezadmin/dumpconfig'.format(args.auto_fetch[0])
        ez_conf = requests.get(req_url, auth=(args.auto_fetch[1],args.auto_fetch[2])).json()
        name = req_url.split('.')[0].split('//')[1]

        pg_dsn = ez_conf['PDO_DATA_DSN'].split(':')[1].replace(';',' ')

        sqlite_file = ez_conf['PDO_DESIGN_DSN'].split(':')[1]

        eas_url = ez_conf['EAS_INTERNAL_URL']

        eas_instance =  ez_conf['INSTANCE']

    if args.name_in_source is not None:
        name = args.name_in_source

    if args.easydb_pg_dsn is not None:
        pg_dsn = args.easydb_pg_dsn

    if args.easydb_sqlite_file:
        sqlite_file = args.easydb_sqlite_file

    if args.easydb_eas_url is not None:
        eas_url = args.easydb_eas_url

    if args.easydb_eas_instance is not None:
        eas_instance =  args.easydb_eas_instance

    source_file=args.source

    extract.__pg_init()
    extract.__sqlite_init()

    extract.prepare_source(source_file, init=args.init)

    eadb_link_index = """
    CREATE INDEX "%TABLE_NAME_IN_SOURCE%_idx"
    ON "%TABLE_NAME_IN_SOURCE%" (from_table_id, to_table_id, from_id, to_id);
    """

    if sqlite_file is not None:
        logging.info("Adding sqlite to Source")
        extract.sqlite_to_source(
            name=name,
            filename=sqlite_file
            )

    logging.info("Adding POSTGRES to Source (migration)")
    extract.pg_to_source(
        name=name,
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

    if eas_versions is not None:
        extract.eas_to_source(
            name=name,
            url=eas_url,
            instance=eas-instance,
            eas_versions=eas_versions
            )

    extract.__commit_source()

if args.mode=="ez_export":

    source_file=args.output

    extract.__pg_init()
    extract.__sqlite_init()

    extract.prepare_source(source_file, init=args.init)

if args.mode=="file_import":

    source_file=args.output

    extract.__pg_init()
    extract.__sqlite_init()

    extract.prepare_source(source_file, init=args.init)
