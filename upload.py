#!/usr/bin/python3

import logging
import logging.config
import sys


import easydb.migration.load.job

# setup

job = easydb.migration.load.job.LoadJob.create_job('Load lette-verein')#change job name accordingly, even though it doesnt matter

#setting up logging, can be left like that
standard_formatter = logging.Formatter('%(asctime)s %(levelname)s [%(name)s] %(message)s', '%Y.%m.%d %H:%M:%S')
user_formatter = logging.Formatter('%(message)s')
root_logger = logging.getLogger()
root_logger.setLevel('DEBUG')
user_logger = logging.getLogger('user')
user_logger.setLevel('DEBUG')

console_log = logging.StreamHandler()
console_log.setLevel(logging.DEBUG)
console_log.setFormatter(standard_formatter)
root_logger.addHandler(console_log)

migration_log = logging.FileHandler('{}/migration.log'.format(job.destination_dir))
migration_log.setLevel(logging.DEBUG)
migration_log.setFormatter(standard_formatter)
root_logger.addHandler(migration_log)

user_log = logging.FileHandler('{}/user.log'.format(job.destination_dir))
user_log.setLevel(logging.DEBUG)
user_log.setFormatter(user_formatter)
user_logger.addHandler(user_log)

logging.getLogger('easydb.server').setLevel('WARN')
logging.getLogger('requests').setLevel('WARN')
logging.getLogger('easydb.repository').setLevel('WARN')
logging.getLogger('easydb.migration.load.loader').setLevel('INFO')


# add all tables (just names without easydb in front), that are supposed to be uploaded
# dont include: link-tables (example__linked_object), file and system tables

objecttypes = [
     'ez_group', 'ez_user', 'ez_pool', 'ez_collection'
]
job.load(objecttypes)

#####INSERT CUSTOM OBJECT-TYPES HERE
objecttypes = []
job.load(objecttypes)#incase https is necessary call with "verify_ssl=False" ##to skip Searching for existing assets call with "search_assets=False"

objecttypes = [
     'ez_collection__objects'
]
job.load(objecttypes)
job.log_times()
