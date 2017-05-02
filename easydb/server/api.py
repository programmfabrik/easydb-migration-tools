'''
* easydb-migration-tools - easydb Migration Tools
 * Copyright (c) 2013 - 2016 Programmfabrik GmbH
 * MIT Licence
 * https://github.com/programmfabrik/coffeescript-ui, http://www.coffeescript-ui.org
'''

import logging
import requests
import json
import urllib3

from easydb.tool.json import parse_json, extract_from_json


from distutils.version import LooseVersion
if LooseVersion(urllib3.__version__) >= LooseVersion('1.9'):
    # jessie version trying to be smart and doing RFC 2231 encoding
    # for "filename" parameter (see #30440)
    # prohibited by:
    # (http://www.w3.org/html/wg/drafts/html/master/semantics.html#multipart-form-data)
    import six, email.utils
    # from /usr/lib/python3/dist-packages/urllib3/fields.py
    def x_format_header_param(name, value):
        if not any(ch in value for ch in '"\\\r\n'):
            result = '%s="%s"' % (name, value)
            try:
                result.encode('ascii')
            except UnicodeEncodeError:
                pass
            else:
                return result
        if not six.PY3:  # Python 2:
            value = value.encode('utf-8')
        value = '%s="%s"' % (name, value)
        return value

    # monkey-patch "requests" module
    requests.packages.urllib3.fields.format_header_param = x_format_header_param


def authenticated(f):
    def new_f(self, *args, **kwargs):
        if not self.is_authenticated():
            raise Exception('EasydbAPI is not authenticated')
        return f(self, *args, **kwargs)
    return new_f

def parsed(f):
    def new_f(self, *args, **kwargs):
        r = f(self, *args, **kwargs)
        if r.status_code != 200:
            raise Exception('Easydb returned an error: {0}'.format(r.text))
        return json.loads(r.text)
    return new_f

class EasydbAPI(object):

    def __init__(self, url, auth=None):
        self.token = None
        self.url = url
        self.logger = logging.getLogger('easydb.server.api')
        self.root_pool = None
        self.auth = auth

    def authenticate(self, login, password):
        self.logger.info('authenticate')
        session = self._get('session')
        token = parse_json(session, {'token':'$token'})['token']
        session_params = {
            'token': token,
            'login': login,
            'password': password
        }
        session = self._post('session/authenticate', params=session_params)
        self.token = parse_json(session, {'token':'$token', 'authenticated': {'method':'easydb'}})['token']

    def is_authenticated(self):
        return self.token is not None

    @authenticated
    def get(self, what, params={}):
        p = self.add_token_param(params)
        return self._get(what, p)

    @parsed
    def _get(self, what, params={}):
        self.logger.debug('get {0} - {1}'.format(what, params))
        url = self.get_url(what)
        return requests.get(url, params=params, auth=self.auth)

    @authenticated
    def post(self, what, js=None, params={}, files=None):
        p = self.add_token_param(params)
        return self._post(what, js, p, files)

    @parsed
    def _post(self, what, js=None, params={}, files=None):
        data = js_body(js) if js is not None else None
        self.logger.debug('post {0} - {1}:\n{2}'.format(what, params, data))
        url = self.get_url(what)
        headers = js_header() if not files else None
        return requests.post(url, params=params, data=data, headers=headers, files=files, auth=self.auth)

    @authenticated
    @parsed
    def put(self, what, js, params={}):
        url = self.get_url(what)
        p = self.add_token_param(params)
        return requests.put(url, params=p, data=js_body(js), headers=js_header(), auth=self.auth)

    @authenticated
    @parsed
    def delete(self, what, js=None, params={}):
        url = self.get_url(what)
        p = self.add_token_param(params)
        if js:
            return requests.delete(url, params=p, data=js_body(js), headers=js_header(), auth=self.auth)
        else:
            return requests.delete(url, params=p, auth=self.auth)

    @authenticated
    def create_objects(self, objects):
        if len(objects) == 0:
            return
        js = []
        objecttype = None
        for o in objects:
            if objecttype is None:
                objecttype = o.objecttype
            elif o.objecttype.name != objecttype.name:
                raise Exception('can only batch objects from the same objecttype')
            ojs = o.to_json()
            js.append(ojs)
        self.logger.debug('create objects:\n{0}'.format(json.dumps(js, indent=4)))
        response_objects = self.post('db/{0}'.format(objecttype.name), js, {'priority': '-1', 'format': 'short'})
        if len(response_objects) != len(objects):
            raise Exception('response objects are different from pushed objects')
        for i in range(len(objects)):
            objects[i].id = extract_from_json(response_objects[i], '{0}._id'.format(objecttype.name))
            objects[i].global_object_id = extract_from_json(response_objects[i], '_global_object_id')

    @authenticated
    def create_pools(self, pools):
        if len(pools) == 0:
            return
        js = []
        for pool in pools:
            js.append(pool.to_json(self.get_root_pool()))
        self.logger.debug('PUSH pools:\n{0}'.format(json.dumps(js, indent=4)))
        response_objects = self.post('pool', js)
        if len(response_objects) != len(pools):
            raise Exception('response pools are different from pushed pools')
        for i in range(len(pools)):
            pools[i].id = extract_from_json(response_objects[i], 'pool._id')

    @authenticated
    def create_collections(self, collections):
        if len(collections) == 0:
            return
        for collection in collections:
            self.logger.info("PUT {}".format(collection.displayname['de-DE']))
            response_object=self.put('collection', collection.to_json())
            self.logger.debug('PUSH collection:\n{0}'.format(extract_from_json(response_object, 'collection.displayname:de-DE')))
            collection.id = extract_from_json(response_object, 'collection._id')

    @authenticated
    def create_collection_objects(self, collection_objects):
        if len(collection_objects) == 0:
            return
        for collection_object in collection_objects:
            if collection_object.object_goid==None:
                continue
            call="collection/objects/{}".format(collection_object.collection_id)
            self.logger.info("POST {}".format(call))
            js = collection_object.to_json()
            print(js)
            response_object = self.post(call, collection_object.to_json())
            self.logger.debug('RESPONSE COLLECTION UPDATE:\n {0}'.format(response_object))
            collection_object.uploaded = 'yes'

    @authenticated
    def create_groups(self, groups):
        if len(groups) == 0:
            return
        js = list(map(lambda group : group.to_json(), groups))
        self.logger.debug('PUSH groups:\n{0}'.format(json.dumps(js, indent=4)))
        response_objects = self.post('group', js)
        if len(response_objects) != len(groups):
            raise Exception('response groups are different from pushed groups')
        for i in range(len(groups)):
            groups[i].id = extract_from_json(response_objects[i], 'group._id')

    @authenticated
    def create_users(self, users):
        if len(users) == 0:
            return
        js = list(map(lambda user : user.to_json(), users))
        self.logger.debug('PUSH users:\n{0}'.format(json.dumps(js, indent=4)))
        response_objects = self.post('user', js)
        if len(response_objects) != len(users):
            raise Exception('response users are different from pushed users')
        for i in range(len(users)):
            users[i].id = extract_from_json(response_objects[i], 'user._id')

    def __str__(self):
        return self.url

    # internal

    def get_url(self, call):
        return '{0}/api/v1/{1}'.format(self.url, call)

    def add_token_param(self, params={}):
        params['token'] = self.token
        return params

    def get_root_pool(self):
        if self.root_pool is None:
            res = self.post('search', {'type': 'pool', 'search': [ { 'type': 'in', 'fields': ['pool._id_parent'], 'in': [None] } ] })
            pools = extract_from_json(res, 'objects')
            if len(pools) == 0:
                raise Exception('failed to retrieve root pool')
            self.root_pool = extract_from_json(pools[0], 'pool._id')
        return self.root_pool

# helpers

def js_body(js):
    return json.dumps(js, indent=4)

def js_header(header={}):
    header['content-type'] = 'application/json'
    return header
