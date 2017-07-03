'''
* easydb-migration-tools - easydb Migration Tools
 * Copyright (c) 2013 - 2016 Programmfabrik GmbH
 * MIT Licence
 * https://github.com/programmfabrik/coffeescript-ui, http://www.coffeescript-ui.org
'''

import json

class Pool(object):

    def __init__(self):
        self.name = {}
        self.description = {}
        self.parent_id = None
        self.id = None
        self.version = 1
        self.source_id = None
        self.standard_masks = None
        self.shortname=None

    def to_json(self, root_pool_id):
        js = {
            'pool': {
                '_id': self.id,
                '_version': self.version,
                'name': self.name,
                'description': self.description
                'shortname': self.shortname,
            }
        }
        if self.parent_id is None:
            js['pool']['_id_parent'] = root_pool_id
        else:
            js['pool']['_id_parent'] = self.parent_id
        if self.standard_masks is not None:
            js['_standard_masks'] = self.standard_masks
        return js

    @staticmethod
    def from_row(row):
        pool = Pool()
        for key, value in row.items():
            if key.startswith('name:'):
                pool.name[key.split(':')[1]] = value
            if key.startswith('description:'):
                pool.description[key.split(':')[1]] = value
            if key == '__parent_id':
                pool.parent_id = value
            if key == '__source_unique_id':
                pool.source_id = value
            if key == '_standard_masks' and value is not None:
                pool.standard_masks = json.loads(value)
            if key == 'shortname':
                pool.shortname = value
        return pool
