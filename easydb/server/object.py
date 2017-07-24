'''
* easydb-migration-tools - easydb Migration Tools
 * Copyright (c) 2013 - 2016 Programmfabrik GmbH
 * MIT Licence
 * https://github.com/programmfabrik/coffeescript-ui, http://www.coffeescript-ui.org
'''

from easydb.server.datamodel import *

class Object(object):

    def __init__(self, objecttype, mask=None):
        self.objecttype = objecttype
        self.mask = '_all_fields' if mask is None else mask
        self.id = None
        self.global_object_id = None
        self.version = 1
        self.fields = {}
        self._comment = None
        self._parent_id = None
        self._pool_id = None
        self._tags = []
        self.collection_type=None

    def to_json(self, as_nested=False):
        js = {}
        if not as_nested:
            js['_mask'] = self.mask
            js[self.objecttype.name] = {}
            js[self.objecttype.name]['_id'] = self.id
            js[self.objecttype.name]['_version'] = self.version
            if self._parent_id:
                js[self.objecttype.name]['_id_parent'] = self._parent_id
            if self._comment is not None:
                js['_comment'] = self._comment
            if self.objecttype.pool_link:
                if self._pool_id is None:
                    raise Exception('object without pool')
                js[self.objecttype.name]['_pool'] = {
                    'pool': {
                        '_id': self._pool_id
                    }
                }
            if len(self._tags) > 0:
                js['_tags'] = []
                for tag in self._tags:
                    js['_tags'].append({'_id': tag})
        for (name, value) in list(self.fields.items()):
            if name not in self.objecttype.columns:
                raise Exception('field {0} not found'.format(name))
            column = self.objecttype.columns[name]
            self.add_value(js, column, value, as_nested)
        return js

    def add_value(self, js, column, value, as_nested):
        if column.kind == 'link':
            if value is not None:
                value = list(map(lambda o: o.to_json(True) if isinstance(o, Object) else o, value))
        elif column.kind == 'column':
            if column.column_type == 'link':
                for c in self.objecttype.constraints:
                    if isinstance(c, ForeignKeyConstraintDefinition) and column.name in c.own_columns:
                        if value is not None:
                            value = {
                                c.ref_objecttype: {
                                    '_id': value
                                }
                            }
                        break
                else:
                    raise Exception('FKC not found')
            elif column.column_type in {'date', 'datetime'}:
                # TODO: check date format
                if value is not None and len(value) > 0:
                    value = { 'value': value }
                else:
                    value = None
            elif column.column_type == 'decimal.2':
                if value is not None:
                    value = float(value)
            elif column.column_type == 'daterange':
                if value is not None:
                    date_from=value.split("|")[0]
                    date_to=value.split("|")[1]
                    value={"from": "{}".format(date_from),"to": "{}".format(date_to)}
            elif column.column_type == 'boolean':
                if value is None:
                    value = False
            elif column.column_type == 'integer.2':
                if value is None or value == '':
                    value = None
                else:
                    value = int(value)
            elif  'l10n' in column.column_type:
                value=value
        else:
            raise Exception('column kind {0} not supported'.format(column.kind))
        if as_nested:
            js[column.name] = value
        else:
            js[self.objecttype.name][column.name] = value
