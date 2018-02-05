'''
* easydb-migration-tools - easydb Migration Tools
 * Copyright (c) 2013 - 2016 Programmfabrik GmbH
 * MIT Licence
 * https://github.com/programmfabrik/coffeescript-ui, http://www.coffeescript-ui.org
'''

__all__ = [
    'TransformJob',
]

import logging
import time
import argparse

import easydb.server.api
import easydb.migration.transform.prepare
import easydb.migration.transform.extractors
import easydb.migration.transform.common
import easydb.migration.transform.extract

logger = logging.getLogger('easydb.migration.transform.job')

class TransformJob(object):

    def __init__(self, easydb_url, login, password, source_dir, destination_dir, create_policy, init, exit_on_error=True, *args, **kwargs):
        self.exit_on_error = exit_on_error
        self.easydb_api = easydb.server.api.EasydbAPI(easydb_url)
        self.easydb_api.authenticate(login, password)
        self.time_info = []
        self.source_dir = source_dir
        self.destination_dir = destination_dir
        self.create_policy = create_policy
        self.source = None
        self.init = init

    def __del__(self):
        if self.source is not None and self.source.is_open():
            self.source.close()

    def prepare(self, *args, **kwargs):
        start = time.time()
        self.destination, self.source = easydb.migration.transform.prepare.prepare(self.easydb_api, self.destination_dir, self.source_dir, self.create_policy, *args, **kwargs)
        if self.init:
            exit()
        self.source.open()
        end = time.time()
        self.log_time('prepare', start, end)

    def extract(self, extractor, destination_table, row_transformations=[], asset_columns=[], batch_size=1000, stop_on_error=None, defer_foreign_keys=False):
        start = time.time()
        if stop_on_error is None:
            stop_on_error = self.exit_on_error
        try:
            easydb.migration.transform.extract.extract(self.source, self.destination, extractor, destination_table, row_transformations, asset_columns, batch_size, stop_on_error, defer_foreign_keys)
        except easydb.migration.transform.common.MigrationStop:
            if self.exit_on_error:
                exit(1)
        end = time.time()
        self.log_time('extract - {0}'.format(extractor), start, end)

    def extract_list(self, rows, destination_table, *args, **kwargs):
        self.extract(easydb.migration.transform.extractors.ExtractList(rows, destination_table), destination_table, *args, **kwargs)

    def extract_sql(self, sql, destination_table, *args, **kwargs):
        self.extract(easydb.migration.transform.extractors.ExtractSQL(self.source, sql, destination_table), destination_table, *args, **kwargs)

    def log_times(self):
        logger.info('[time] summary')
        total_time_ms = 0
        for (what, time_ms) in self.time_info:
            logger.info('[time] {0}: {1}'.format(what, time_ms))
            total_time_ms += time_ms
        logger.info('[time] total: {0}'.format(total_time_ms))

    def log_time(self, what, start, end):
        time_ms = round((end - start) * 1000)
        logger.info('[time] {0}: {1}'.format(what, time_ms))
        self.time_info.append((what, time_ms))

    @staticmethod
    def create_job(job_name, create_policy):
        argparser = TransformJob.get_argparser(job_name)
        a = argparser.parse_args()
        return TransformJob(a.url, a.login, a.password, a.source, a.destination, create_policy, a.init)

    @staticmethod
    def get_argparser(job_name):
        argparser = argparse.ArgumentParser(description=job_name)
        argparser.add_argument('url',          help='easydb URL')
        argparser.add_argument('source',       help='source directory')
        argparser.add_argument('destination',  help='destination directory')
        argparser.add_argument('--login',      help='easydb login', default='root')
        argparser.add_argument('--password',   help='easydb password', default='admin')
        argparser.add_argument('--init',       action='store_true',   help='Only prepare empty destination-db')
        return argparser

