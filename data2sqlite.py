#!/usr/bin/python3
# coding=utf8

import sys
import argparse
import os
import easydb.migration.extract as extract
import json
import logging
import logging.config
import requests

logging.basicConfig(level=logging.WARN)

argparser = argparse.ArgumentParser(description='data2qlite')

argparser.add_argument('-t', '--target', default='dump.db',                     help='Sqlite-Dump name and directory (Default: ./dump.db)')
argparser.add_argument('--init', action='store_true',                           help='If set, existing files will be purged')
argparser.add_argument('--name', default='source',                              help='NAME in target db (required, default: "source")')
argparser.add_argument('--dump_mysql',                                          help='If set, output in mysql sql format, use "-" to dump to STDOUT')
argparser.add_argument('-s', '--silent', action='store_true',                   help="If set, don't output progress every 100 rows.")

subparsers=argparser.add_subparsers(help="Set Datasources", dest='mode')

migration_parser=subparsers.add_parser('easydb4',                               help="Set Migration Mode to create Source for Migration")
migration_parser.add_argument('--config', nargs=3,                              help='Fetch Server Information from URL, usage: "--config URL login password" If set, no other arguments need to be set.')
migration_parser.add_argument('--pg_dsn',                                       help='DSN for easydb-PostgreSQL Server, must be sperated by spaces')
migration_parser.add_argument('--sqlite_file',                                  help='Filename for easydb_SQLite Database')
migration_parser.add_argument('--eas_url',                                      help='URL for easydb-EAS-Server')
migration_parser.add_argument('--eas_instance',                                 help='Instance-Name on EAS-Server')
migration_parser.add_argument('--eas_versions',  nargs='*',                     help='Asset Version and Storage-Method, enter "version:method", e.g "original:url"')
migration_parser.add_argument('--schema', default='public',                     help='Schema for pg-database, default = "public". Set to "none" to not use a schema.')

pg_parser=subparsers.add_parser('pg', help="Add to Source from postgres")
pg_parser.add_argument('--dsn',                                                 help='DSN for PostgreSQL,format: "dbname=easydb port=5432 user=postgres"')
pg_parser.add_argument('--schema', default='public',                            help='Schema for pg-database, default = "public"')
pg_parser.add_argument('--tables', nargs='*', default=[],                       help='Select Tables for Export from postgresql')

mysql_parser=subparsers.add_parser('mysql', help="Add to Source from mySQL")
mysql_parser.add_argument('--host',                                                help='mySQL host')
mysql_parser.add_argument('--dbname',                                              help='DB in mySQL host')
mysql_parser.add_argument('--username',                                            help='Username for mySQL-DB')
mysql_parser.add_argument('--password', default='',                                help='PW for mySQL-User')
mysql_parser.add_argument('--tables', nargs='*', default=[],                       help='Select Tables for Export from postgresql')

import_parser=subparsers.add_parser('file', help="Add to Source from other files")
import_parser.add_argument('--sqlite', nargs='*', default=[],                   help='Filename for SQLite Database')
import_parser.add_argument('--XML', nargs='*', default=[],                      help='Filename(s) for XML')
import_parser.add_argument('--CSV', nargs='*', default=[],                      help='Filename(s) for CSV')
import_parser.add_argument('--XLSX', nargs='*', default=[],                     help='Filename(s) for Excel (Supported formats are .xlsx, .xlsm, .xltx, .xltm)')

import_parser=subparsers.add_parser('adhh', help="Add to Source from ADHH XML files")
import_parser.add_argument('--sqlite', nargs='*', default=[],                   help='Filename for SQLite Database')
import_parser.add_argument('--xml', nargs='*', default=[],                      help='Filename(s) for ADHH XML')

global args

args = argparser.parse_args()

extract.__pg_init()
extract.__sqlite_init()
extract.prepare_source(args.target, init=args.init)

extract.args = args

##MIGRATION#####################################################################
if args.mode=="easydb4":
    eas_versions = None
    pg_dsn = None
    name="blank"
    sqlite_file=None
    eas_url=None
    eas_instance=None
    eas_versions=None
    schema=None

    if args.config is not None:
        req_url='{0}/ezadmin/dumpconfig'.format(args.config[0])
        _res = requests.get(req_url, auth=(args.config[1],args.config[2]))

        ez_conf = extract.res_get_json(_res)

        name = req_url.split('.')[0].split('//')[1]

        pg_dsn = ez_conf['PDO_DATA_DSN'].split(':')[1].replace(';',' ')

        sqlite_file = ez_conf['PDO_DESIGN_DSN'].split(':')[1]

        eas_url = ez_conf['EAS_EXTERNAL_URL']

        eas_instance = ez_conf['INSTANCE']

        eas_versions = {'original': ['url']}
    if args.name is not None:
        name = args.name
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
    if args.schema:
        schema = args.schema

    if not schema:
        schema = 'public'

    if schema == 'none':
        schema = None

    if args.eas_url is not None:
        eas_url = args.eas_url

        if args.eas_instance is None:
            logging.warning('No eas_instance provided. Program will terminate now')
            sys.exit(0)

        eas_instance =  args.eas_instance

        if args.eas_versions is None:
            logging.warning('No eas_versions provided. Program will terminate now')
            sys.exit(0)

        eas_versions={}
        for version in args.eas_versions:
            split = version.split(":")
            if split[0] in eas_versions:
                if split[1] not in eas_versions[split[0]]:
                    eas_versions[split[0]].append(split[1])
            else:
                eas_versions[split[0]]=[split[1]]

        print("eas-info: \n")
        print(("URL: " + eas_url))
        print(("Instance: " + eas_instance))
        print(("VERSIONS: " + str(eas_versions)))

    print(("\nsqlite-file: %s" % sqlite_file))
    print(("PG-DSN: %s" % pg_dsn))
    print(("Schema: %s" % schema))
    sys.stdout.flush()

    eadb_link_index = """
    CREATE INDEX "%TABLE_NAME_IN_SOURCE%_idx"
    ON "%TABLE_NAME_IN_SOURCE%" (from_table_id, to_table_id, from_id, to_id);
    """
    logging.info("Adding to Source from sqlite")
    extract.sqlite_to_source(
        name=name,
        filename=sqlite_file
        )

    if schema != None:
        schema_prefix = schema+"."
    else:
        schema_prefix = ""

    logging.info("Adding to Source from pg")
    extract.pg_to_source(
        name=name,
        schema_name=schema,
        dsn=pg_dsn,
        include_tables_exclusive=False,
        include_tables = {
        schema_prefix+'eadb_links': {
            'onCreate': eadb_link_index
            }
        }
        # ,
        # exclude_tables = [
        #     'public.eadb_changelog',
        #     'public.eadb_table_sql_changelog',
        #     'public.eadb_rights'
        #     ]
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

    if args.XLSX != []:
        for xlsx_file in args.XLSX:
            logging.info("Adding XLSX to Source")
            extract.excel_to_source(
                name=args.name,
                filename=xlsx_file)

##ADHH-IMPORT##################################################################
if args.mode=="adhh":

    source_file=args.target

    if args.sqlite !=[]:
        for sqlite_file in args.sqlite:
            extract.__sqlite_init()
            logging.info("Adding sqlite '" + sqlite_file + "' to Source")
            extract.sqlite_to_source(
                name=args.name,
                filename=sqlite_file
                )

    if args.xml !=[]:
        paths=[]
        for xml_file in args.xml:
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

        for xml_file in args.xml:
            logging.info("Adding ADHH XML '" + xml_file + "' to Source")
            extract.adhh_xml_to_source(
                name=args.name,
                filename=xml_file,
                basedir=basedir
                )


if args.dump_mysql is not None:
    extract.dump_mysql(
            output=args.dump_mysql
            )
extract.__commit_source()
