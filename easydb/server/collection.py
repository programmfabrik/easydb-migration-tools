'''
* easydb-migration-tools - easydb Migration Tools
 * Copyright (c) 2013 - 2016 Programmfabrik GmbH
 * MIT Licence
 * https://github.com/programmfabrik/coffeescript-ui, http://www.coffeescript-ui.org
'''

import json

class Collection(object):

    def __init__(self):
        self.displayname = {}
        self.description= {}
        self.parent_id = None
        self.id = None
        self.version = 1
        self.source_id = None
        self.owner = None
        self.owner_id=None
        self.user_collection_id=None
        self.type=None
        

    def to_json(self):
        if self.user_collection_id is not None:
            js = {
                '_basetype': 'collection',
                'collection': {
                    '_id': self.id,
                    '_version': self.version,
                    'displayname': self.displayname,
                    'description': self.description,
                    '_id_parent': self.user_collection_id,
                    'children_allowed': True,
                    'type': self.type
                    },
                '_owner': {
                    '_basetype': 'user',
                    'user':{
                    '_id': self.owner_id
                    }
                }
                }
        if self.user_collection_id is None:
            js = {
                '_basetype': 'collection',
                'collection': {
                    '_id': self.id,
                    '_version': self.version,
                    'displayname': self.displayname,
                    'description': self.description,
                    '_id_parent': self.parent_id,
                    'children_allowed': True,
                    'type': self.type
                    },
                '_owner': {
                    '_basetype': 'user',
                    'user':
                        {
                        '_id': self.owner_id
                        }
                    }
                }

        return js

    @staticmethod
    def from_row(row):
        collection = Collection()
        for key, value in row.items():
            if key.startswith('displayname:'):
                collection.displayname[key.split(':')[1]] = value
            if key.startswith('description:'):
                collection.description[key.split(':')[1]] = value
            if key == '__parent_id':
                collection.parent_id = value
            if key == '__source_unique_id':
                collection.source_id = value
            if key == '__owner':
                collection.owner = value
            if key == '__owner_id':
                collection.owner_id=value
            if key == '__user_collection_id':
                collection.user_collection_id=value
            if key == '__type':
                collection.type = value
        return collection
