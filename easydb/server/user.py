'''
* easydb-migration-tools - easydb Migration Tools
 * Copyright (c) 2013 - 2016 Programmfabrik GmbH
 * MIT Licence
 * https://github.com/programmfabrik/coffeescript-ui, http://www.coffeescript-ui.org
'''

import json

class User(object):

    def __init__(self):
        self.id = None
        self.version = 1
        self.last_name = None
        self.first_name = None
        self.remarks = None
        self.login = None
        self.email = None
        self.phone = None
        self.street = None
        self.postal_code = None
        self.town = None
        self.country = None
        self.password = None
        self.frontend_prefs = None
        self.login_disabled = False
        self.groups = []
        self.source_id = None

    def to_json(self):
        js = {
            'user': {
                '_id': self.id,
                '_version': self.version,
                'last_name': self.last_name,
                'first_name': self.first_name,
                'remarks': self.remarks,
                'login': self.login,
                'phone': self.phone,
                'street': self.street,
                'postal_code': self.postal_code,
                'town': self.town,
                'country': self.country,
                'frontend_prefs': self.frontend_prefs,
                'login_disabled': self.login_disabled
            },
            '_groups': list(map(lambda gid: { 'group': {'_id': int(gid)} }, self.groups))
        }
        if self.email is not None:
            js["_emails"] = [
                {
                    "needs_confirmation":False,
                    "email": self.email,
                    "use_for_login": True,
                    "use_for_email": True,
                    "send_email": False,
                    "send_email_include_password": False,
                    "is_primary": True,
                    "intended_primary": False
                }
            ]
        if self.password is not None:
            js['_password'] = self.password
        return js

    @staticmethod
    def from_rows(row, group_rows):
        user = User()
        for key, value in row.items():
            if key == 'last_name':
                user.last_name = value
            elif key == 'first_name':
                user.first_name = value
            elif key == 'remarks':
                user.remarks = value
            elif key == 'login':
                user.login = value
            elif key == 'phone':
                user.phone = value
            elif key == 'email':
                user.email = value
            elif key == 'street':
                user.street = value
            elif key == 'postal_code':
                user.postal_code = value
            elif key == 'town':
                user.town = value
            elif key == 'country':
                user.country = value
            elif key == 'password':
                user.password = value
            elif key == 'frontend_prefs':
                if value is not None:
                    user.frontend_prefs = json.loads(value)
            elif key == 'login_disabled':
                if value is not None:
                    user.login_disabled = value
            elif key == '__source_unique_id':
                user.source_id = value
        for row in group_rows:
            if row['group_id'] is not None:
                user.groups.append(row['group_id'])
        return user
