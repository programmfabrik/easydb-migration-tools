#!/usr/bin/python
# coding=utf8

# This file may contain credentials - do not leave the file on foreign/customer-servers

# notes for LINUX (debian 7 and 8 a.k.a. wheezy and jessie, they have python 2.7)
#   apt-get install python-requests python-mysqldb  # on a typical easydb4 production machine
#   apt-get install python-psycopg2 python-chardet  # these two were already present

# notes for WINDOWS install:
#
#  required: 32BIT version and the (default) install path c:\Python2.7
#
#  download and install:
# https://www.python.org/ftp/python/2.7.9/python-2.7.9.msi
# https://pypi.python.org/packages/2.7/M/MySQL-python/MySQL-python-1.2.5.win32-py2.7.exe
# http://www.stickpeople.com/projects/python/win-psycopg/2.6.0/psycopg2-2.6.0.win32-py2.7-pg9.4.1-release.exe
#
#  then in CMD:
# pip install requests
# pip install chardet

# what is this paragraph? example usage? all valid cmdline-arguments? please replace me with a good header
#
# --eas-to-source='name="stadt-mannheim", url="http://easdb.4.0.mad.pf-berlin.de/eas", instance="stadt-mannheim", eas_version="thumbnail"'
# --load-from-file "..."
# write into "protocol" table
# --use "<mysourcefile>.sqlite" --dump-protocol  : reads from sqlite source and dump protocol table
# --init "<mysourcefile>.sqlite"

import os
import sys
import argparse

from common import common

common.__pg_init()
common.__sqlite_init()

parser = argparse.ArgumentParser('Run hardcoded to-do from this script.')
parser.add_argument('script', help='Script to run')

parser.add_argument('--source', dest='source', action='store',
                    help='source to use, init is set to False, if file exists, to True otherwise.')

parser.add_argument('--init', dest='init', action='store_const', const=True, default=False,
                    help='if source exists, force init.')

parser.add_argument('--init-filestore', dest='init_filestore', action='store_const', const=True, default=None,
                    help='if source exists, purge filestore.')

parser.add_argument('--commit-on-error', dest='commit_on_error', action='store_const', const=True, default=False,
                    help='if set, commit db on error, useful for debugging.')

args = parser.parse_args()

if args.source:
    fn = args.source

    if args.init:
        init = True
    else:
        init = not os.path.isfile(fn)

    if args.init_filestore == False:
        init_filestore = False
    elif args.init_filestore == True:
        init_filestore = True
    else:
        init_filestore = None

    common.prepare_source(
        source = fn,
        init = init,
        init_filestore = init_filestore
        )

path = os.path.abspath(os.path.dirname(sys.argv[0]))
sys.path.append(path)

print ('Running script {}'.format(args.script))

script_path = os.path.abspath(os.path.dirname(args.script))
sys.path.append(script_path)
module = os.path.splitext(os.path.basename(args.script))[0]

__import__(module)

common.__commit_source()
