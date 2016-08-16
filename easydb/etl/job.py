__all__ = [
    'ETLJob',
]

import logging
import time
import argparse

from easydb.etl.common import *
from easydb.etl.prepare import *
from easydb.etl.extract import *
from easydb.etl.load import *
from easydb.etl.extractors import *
from easydb.server.api import *

logger = logging.getLogger('easydb.etl.job')

class ETLJob(object):

    def __init__(self, easydb_url, login, password, eas_url, eas_instance, source_dir, destination_dir, create_policy, http_auth=None, exit_on_error=True, *args, **kwargs):
        self.exit_on_error = exit_on_error
        self.easydb_api = EasydbAPI(easydb_url, http_auth)
        self.easydb_api.authenticate(login, password)
        self.eas_url = eas_url
        self.eas_instance = eas_instance
        self.time_info = []
        self._prepare(source_dir, destination_dir, create_policy, *args, **kwargs)

    def __del__(self):
        if self.source.is_open():
            self.source.close()

    def extract(self, extractor, destination_table, row_transformations=[], asset_columns=[], batch_size=1000, stop_on_error=None, defer_foreign_keys=False):
        start = time.time()
        if stop_on_error is None:
            stop_on_error = self.exit_on_error
        try:
            extract(self.source, self.destination, extractor, destination_table, row_transformations, asset_columns, batch_size, stop_on_error, defer_foreign_keys)
        except ETLStop:
            if self.exit_on_error:
                exit(1)
        end = time.time()
        self.log_time('extract - {0}'.format(extractor), start, end)

    def extract_list(self, rows, destination_table, *args, **kwargs):
        self.extract(ExtractList(rows, destination_table), destination_table, *args, **kwargs)

    def extract_sql(self, sql, destination_table, *args, **kwargs):
        self.extract(ExtractSQL(self.source, sql, destination_table), destination_table, *args, **kwargs)

    def load(self, objecttypes=None, batch_size=1000, search_assets=True, stop_on_error=None, custom_nested_loaders={}):
        start = time.time()
        if stop_on_error is None:
            stop_on_error = self.exit_on_error
        try:
            load(self.source, self.destination, self.easydb_api, self.eas_url, self.eas_instance, objecttypes, custom_nested_loaders, batch_size, stop_on_error, search_assets)
        except ETLStop:
            if self.exit_on_error:
                exit(1)
        end = time.time()
        self.log_time('load', start, end)

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
    def start_job(create_policy, *args, **kwargs):
        argparser = ETLJob.get_argparser()
        a = argparser.parse_args()
        return ETLJob(a.url, a.login, a.password, a.eas_url, a.eas_instance, a.source, a.destination, create_policy, *args, **kwargs)

    @staticmethod
    def get_argparser():
        argparser = argparse.ArgumentParser(description='Run migration script')
        argparser.add_argument('script',       help='migration script')
        argparser.add_argument('url',          help='easydb URL')
        argparser.add_argument('eas_url',      help='EAS URL')
        argparser.add_argument('eas_instance', help='EAS instance')
        argparser.add_argument('source',       help='source directory')
        argparser.add_argument('destination',  help='destination directory')
        argparser.add_argument('--login',      help='easydb login', default='root')
        argparser.add_argument('--password',   help='easydb password', default='admin')
        return argparser

    # private

    def _prepare(self, source_dir, destination_dir, create_policy, *args, **kwargs):
        start = time.time()
        self.destination, self.source = prepare(self.easydb_api, destination_dir, source_dir, create_policy, *args, **kwargs)
        self.source.open()
        end = time.time()
        self.log_time('prepare', start, end)

