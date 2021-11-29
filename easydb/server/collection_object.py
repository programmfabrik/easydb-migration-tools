'''
* easydb-migration-tools - easydb Migration Tools
 * Copyright (c) 2013 - 2016 Programmfabrik GmbH
 * MIT Licence
 * https://github.com/programmfabrik/coffeescript-ui, http://www.coffeescript-ui.org
'''

import json

class Collection_Object(object):

    def __init__(self):
        self.collection_id=None
        self.object_id=None
        self.id = None
        self.version = 1
        self.source_id = None
        self.uploaded=None
        self.object_goid=None
        self.position=0

    def to_json(self):
        js = {
        "objects": [
                    {
                    "_global_object_id": self.object_goid
                    }
                ]
        }
        return js

    @staticmethod
    def from_row(row):
        collection_object = Collection_Object()

        for key, value in list(row.items()):
            if key == '__source_unique_id':
                collection_object.source_id = value
            if key == 'collection_id':
                collection_object.collection_id=value
            if key == 'object_id':
                collection_object.object_id=value
            if key == 'uploaded':
                collection_object.uploaded=value
            if key == 'object_goid':
                collection_object.object_goid=value
            if key == 'positon':
                collection_object.position=value
        return collection_object
