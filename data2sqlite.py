#!/usr/bin/python3
# coding=utf8

import sys
import argparse
import os
import easydb.migration.extract as extract
import logging
import logging.config
import requests

logging.basicConfig(level=logging.WARN)

argparser = argparse.ArgumentParser(description='data2qlite')

argparser.add_argument('-t', '--target', default='dump.db',
                       help='Sqlite-Dump name and directory (Default: ./dump.db)')
argparser.add_argument('--init', action='store_true',
                       help='If set, existing files will be purged')
argparser.add_argument('--name', default='source',
                       help='NAME in target db (required, default: "source")')
argparser.add_argument('-s', '--silent', action='store_true',
                       help='If set, don\'t output progress every 100 rows.')

subparsers = argparser.add_subparsers(dest='mode', help='Set Datasources')

import_parser = subparsers.add_parser(
    'file', help='Add to Source from other files')
import_parser.add_argument('--K10plus', nargs='*', default=[],
                           help='Filename(s) for k10plus (PICA+ format, .pp files)')

global args

args = argparser.parse_args()

if args.mode not in [
    'easydb4',
    'pg',
    'mysql',
    'file',
    'adhh',
]:
    argparser.print_usage(sys.stdout)
    sys.exit()

extract.__sqlite_init()
extract.prepare_source(args.target, init=args.init)

extract.args = args


# FILE-IMPORT


if args.mode == 'file':

    source_file = args.target


    if args.K10plus != []:
        for k10plus_file in args.K10plus:
            item_id_offset, offset = extract.k10plus_get_offset(k10plus_file)
            logging.info('Adding "' + k10plus_file +
                         '" to Source (start offset:' + str(offset) + ')')
            item_id_offset = extract.k10plus_to_source(
                filename=k10plus_file,
                item_id_offset=item_id_offset,
                offset=offset)



extract.__commit_source()
