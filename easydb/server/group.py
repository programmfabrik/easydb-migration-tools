'''
* easydb-migration-tools - easydb Migration Tools
 * Copyright (c) 2013 - 2016 Programmfabrik GmbH
 * MIT Licence
 * https://github.com/programmfabrik/coffeescript-ui, http://www.coffeescript-ui.org
'''

class Group(object):

    def __init__(self):
        self.displayname = {}
        self.comment = None
        self.id = None
        self.version = 1
        self.source_id = None

    def to_json(self):
        js = {
            'group': {
                '_id': self.id,
                '_version': self.version,
                'displayname': self.displayname,
                'comment': self.comment
            }
        }
        return js

    @staticmethod
    def from_row(row):
        group = Group()
        for key, value in row.items():
            if key.startswith('displayname:'):
                group.displayname[key.split(':')[1]] = value
            elif key == 'comment':
                group.comment = value
            elif key == '__source_unique_id':
                group.source_id = value
        return group
