'''
* easydb-migration-tools - easydb Migration Tools
 * Copyright (c) 2013 - 2016 Programmfabrik GmbH
 * MIT Licence
 * https://github.com/programmfabrik/coffeescript-ui, http://www.coffeescript-ui.org
'''

__all__ = [
    'LoadJob',
]

import logging
import time
import argparse

import easydb.server.api
import easydb.migration.load.loader
import easydb.migration.transform.prepare
import easydb.migration.transform.common

logger = logging.getLogger('easydb.migration.load.job')

class LoadJob(object):

    def __init__(self, easydb_url, login, password, eas_url, eas_instance, source_dir, destination_dir, exit_on_error=True, *args, **kwargs):
        self.exit_on_error = exit_on_error
        self.easydb_api = easydb.server.api.EasydbAPI(easydb_url)
        self.easydb_api.authenticate(login, password)
        self.eas_url = eas_url
        self.eas_instance = eas_instance
        self.time_info = []
        self.source_dir = source_dir
        self.destination_dir = destination_dir
        self.source = None

    def __del__(self):
        if self.source is not None and self.source.is_open():
            self.source.close()

    def load(self, objecttypes=None, batch_size=1000, search_assets=True, verify_ssl=False, stop_on_error=None, custom_nested_loaders={}):
        start = time.time()
        if stop_on_error is None:
            stop_on_error = self.exit_on_error
        try:
            easydb.migration.load.loader.load(
                self.source, self.destination, self.easydb_api, self.eas_url, self.eas_instance, objecttypes, custom_nested_loaders, batch_size, stop_on_error, search_assets, verify_ssl)
        except easydb.migration.transform.common.MigrationStop:
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
    def create_job(job_name):
        argparser = LoadJob.get_argparser(job_name)
        a = argparser.parse_args()
        job = LoadJob(a.url, a.login, a.password, a.eas_url, a.eas_instance, a.source, a.destination)
        job.destination, job.source = easydb.migration.transform.prepare.prepare(
            job.easydb_api, job.destination_dir, job.source_dir, easydb.migration.transform.prepare.CreatePolicy.IfNotExists)
        job.source.open()
        return job

    @staticmethod
    def get_argparser(job_name):
        argparser = argparse.ArgumentParser(description=job_name)
        argparser.add_argument('url',          help='easydb URL')
        argparser.add_argument('eas_url',      help='EAS URL')
        argparser.add_argument('eas_instance', help='EAS instance')
        argparser.add_argument('source',       help='source directory')
        argparser.add_argument('destination',  help='destination directory')
        argparser.add_argument('--login',      help='easydb login', default='root')
        argparser.add_argument('--password',   help='easydb password', default='admin')
        return argparser
