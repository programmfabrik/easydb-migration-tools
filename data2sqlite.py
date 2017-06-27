#!/usr/bin/python
# coding=utf8

import sys
import argparse
import os
import easydb.migration.extract as extract
import json
import logging
import logging.config
import requests

logging.basicConfig(level=logging.INFO)

argparser = argparse.ArgumentParser(description='data2qlite')

argparser.add_argument('-t', '--target', default='dump.db',                     help='Sqlite-Dump name and directory (Default: ./dump.db)')
argparser.add_argument('--init', action='store_true',                           help='If set, existing files will be purged')
argparser.add_argument('--name', default='source',                              help='NAME in target db (required, default: "source")')
argparser.add_argument('--dump_mysql',                                          help='If set, output in mysql sql format, use "-" to dump to STDOUT')

subparsers=argparser.add_subparsers(help="Set Datasources", dest='mode')

migration_parser=subparsers.add_parser('easydb4',                               help="Set Migration Mode to create Source for Migration")
migration_parser.add_argument('--config', nargs=3,                              help='Fetch Server Information from URL, usage: "--auto_fetch URL login password" If set no other arguments have to be set')
migration_parser.add_argument('--pg_dsn',                                       help='DSN for easydb-PostgreSQL Server, must be sperated by spaces')
migration_parser.add_argument('--sqlite_file',                                  help='Filename for easydb_SQLite Database')
migration_parser.add_argument('--eas_url',                                      help='URL for easydb-EAS-Server')
migration_parser.add_argument('--eas_instance',                                 help='Instance-Name on EAS-Server')
migration_parser.add_argument('--eas_versions',  nargs='*',                     help='Asset Version and Storage-Method, enter "version:method", e.g "original:url"')

pg_parser=subparsers.add_parser('pg', help="Add to Source from postgres")
pg_parser.add_argument('--dsn',                                                 help='DSN for PostgreSQL,format: "dbname=easydb port=5432 user=postgres"')
pg_parser.add_argument('--schema', default='public',                            help='Schema for pg-database, default = "public"')
pg_parser.add_argument('--tables', nargs='*', default=[],                       help='Select Tables for Export from postgresql')

pg_parser=subparsers.add_parser('mysql', help="Add to Source from mySQL")
pg_parser.add_argument('--host',                                                help='mySQL host')
pg_parser.add_argument('--dbname',                                              help='DB in mySQL host')
pg_parser.add_argument('--username',                                            help='Username for mySQL-DB')
pg_parser.add_argument('--password', default='',                                help='PW for mySQL-User')
pg_parser.add_argument('--tables', nargs='*', default=[],                       help='Select Tables for Export from postgresql')

import_parser=subparsers.add_parser('file', help="Add to Source from other files")
import_parser.add_argument('--sqlite', nargs='*', default=[],                   help='Filename for SQLite Database')
import_parser.add_argument('--XML', nargs='*', default=[],                      help='Filename for XML')
import_parser.add_argument('--CSV', nargs='*', default=[],                      help='Filename for CSV')

args = argparser.parse_args()

extract.__pg_init()
extract.__sqlite_init()
extract.prepare_source(args.target, init=args.init)


##MIGRATION#####################################################################
if args.mode=="easydb4":

    if args.config is not None:
        req_url='{}/ezadmin/dumpconfig'.format(args.config[0])
        ez_conf = requests.get(req_url, auth=(args.config[1],args.config[2])).json()
        name = req_url.split('.')[0].split('//')[1]

        pg_dsn = ez_conf['PDO_DATA_DSN'].split(':')[1].replace(';',' ')

        sqlite_file = ez_conf['PDO_DESIGN_DSN'].split(':')[1]

        eas_url = ez_conf['EAS_INTERNAL_URL']

        eas_instance = ez_conf['INSTANCE']

        eas_versions = {'original': ['url']}

    if args.pg_dsn is not None:
        pg_dsn = args.pg_dsn
    if pg_dsn is None:
        logging.warning('No Postgres-DSN provided. Program will terminate now')
        sys.exit(0)

    if args.sqlite_file:
        sqlite_file = args.sqlite_file
    if sqlite_file is None:
        logging.warning('No sqlite_file provided. Program will terminate now')
        sys.exit(0)

    if args.eas_url is not None:
        eas_url = args.eas_url
    if eas_url is None:
        logging.warning('No eas-server provided. Program will terminate now')
        sys.exit(0)

    if args.eas_instance is not None:
        eas_instance =  args.eas_instance
    if eas_url is None:
        logging.warning('No eas-instance provided. Program will terminate now')
        sys.exit(0)

    if args.eas_versions is not None:
        eas_versions={}
        for version in args.eas_versions:
            split = version.split(":")
            if split[0] in eas_versions:
                if split[1] not in eas_verions[split[0]]:
                    eas_verions[split[0]].append(split[1])
            else:
                eas_versions[split[0]]=[split[1]]
    else:
        eas_versions=None

    print("eas-info: \n")
    print(eas_instance)
    print(eas_url)
    print(eas_versions)
    print("\n sqlite-file: \n")
    print(sqlite_file)
    eadb_link_index = """
    CREATE INDEX "%TABLE_NAME_IN_SOURCE%_idx"
    ON "%TABLE_NAME_IN_SOURCE%" (from_table_id, to_table_id, from_id, to_id);
    """
    logging.info("Adding to Source from sqlite")
    extract.sqlite_to_source(
        name=name,
        filename=sqlite_file
        )

    logging.info("Adding to Source from pg")
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
        logging.info("Adding to Source from EAS")
        extract.eas_to_source(
            name=name,
            url=eas_url,
            instance=eas_instance,
            eas_versions=eas_versions
            )

##PG############################################################################
if args.mode=="pg":

    include_tables={}
    for table in args.tables:
        include_tables[table]={}

    if args.dsn is not None:
        if include_tables != {}:
            extract.pg_to_source(
                name=args.name,
                schema_name=args.schema,
                dsn=args.dsn,
                include_tables_exclusive=True,
                include_tables = include_tables
                )
        else:
            extract.pg_to_source(
                name=args.name,
                schema_name=args.schema,
                dsn=args.dsn,
                include_tables_exclusive=False
                )
    else:
        logging.warning('No Postgres-DSN provided. Program will terminate now')
        sys.exit(0)

##mySQL#########################################################################
if args.mode=="mysql":
    include_tables={}
    for table in args.tables:
        include_tables[table]={}

    if args.host is not None and args.dbname is not None and args.username is not None:
        if include_tables != {}:
            extract.mysql_to_source(
                name=args.name,
                host=args.host,
                db=args.dbname,
                user=args.username,
                passwd=args.password,
                include_tables_exclusive=True,
                include_tables = include_tables
                )
        else:
            extract.mysql_to_source(
                name=args.name,
                host=args.host,
                db=args.dbname,
                user=args.username,
                passwd=args.password,
                include_tables_exclusive=False
                )
    else:
        logging.warning('Information about mySQL-Server is insufficient. Program will terminate now')
        sys.exit(0)
##FILE-IMPORT##################################################################
if args.mode=="file":

    source_file=args.target

    if args.sqlite !=[]:
        for sqlite_file in args.sqlite:
            extract.__sqlite_init()
            logging.info("Adding sqlite to Source")
            extract.sqlite_to_source(
                name=args.name,
                filename=sqlite_file
                )

    if args.XML !=[]:
        paths=[]
        for xml_file in args.XML:
            paths.append(os.path.abspath(xml_file))
        basedir=""
        contains = True
        i=0
        for char in paths[0]:
            for path in paths:
                if path[i] != char:
                    contains=False
            if contains == False:
                break
            else:
                basedir+=char
            i+=1
        basedir_split = basedir.split("/")[1:len(basedir.split("/"))-1]
        basedir = "/"
        for elem in basedir_split:
            basedir+=(elem+"/")

        for xml_file in args.XML:
            logging.info("Adding XML to Source")
            extract.xml_to_source(
                name=args.name,
                filename=xml_file,
                basedir=basedir
                )

    if args.CSV !=[]:
        for csv_file in args.CSV:
            logging.info("Adding CSV to Source")
            extract.csv_to_source(
                name=args.name,
                filename=csv_file,
                )


if args.dump_mysql is not None:
    extract.dump_mysql(
            output=args.dump_mysql
            )
extract.__commit_source()
