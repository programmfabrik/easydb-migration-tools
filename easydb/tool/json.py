'''
* easydb-migration-tools - easydb Migration Tools
 * Copyright (c) 2013 - 2016 Programmfabrik GmbH
 * MIT Licence
 * https://github.com/programmfabrik/coffeescript-ui, http://www.coffeescript-ui.org
'''

from . import json

class AssertError(Exception):
    def __init__(self, error_str):
        self.error_str = error_str
        Exception.__init__(self, error_str)

def build_path(base_path, new_part):
    if isinstance(new_part, str):
        if not base_path:
            return new_part
        else:
            return '{0}.{1}'.format(base_path, new_part)
    if isinstance(new_part, int):
        if not base_path:
            base_path = ''
        return '{0}[{1}]'.format(base_path, new_part)

def compare_json(actual, expected, variables = {}, path = None):
    if isinstance(expected, dict):
        if not isinstance(actual, dict):
            raise AssertError('{0}: expecting object'.format(path))
        for k,v in list(expected.items()):
            required = True
            if k[0] == '$':
                required = False
                ka = k[1:]
            else:
                ka = k
            if ka in actual:
                compare_json(actual[ka], expected[k], variables, build_path(path, k))
            elif required:
                raise AssertError('{0}: expecting {1}'.format(path, k))
    elif isinstance(expected, list):
        if not isinstance(actual, list):
            raise AssertError('{0}: expecting list'.format(path))
        if len(expected) != len(actual):
            raise AssertError('{0}: expecting {1} elements ({2})'.format(path, len(expected), len(actual)))
        for i in range(len(expected)):
            compare_json(actual[i], expected[i], variables, build_path(path, i))
    elif isinstance(expected, str):
        if expected[0] == '$':
            if len(expected) > 1:
                variables[expected[1:]] = actual
        elif not isinstance(actual, str):
            raise AssertError('{0}: expecting string'.format(path))
        elif actual != expected:
            raise AssertError('{0}: "{1}" (expecting "{2}")'.format(path, actual, expected))
    elif isinstance(expected, bool):
        if not isinstance(actual, bool):
            raise AssertError('{0}: expecting bool'.format(path))
        if actual != expected:
            raise AssertError('{0}: {1} (expecting {2})'.format(path, repr(actual), repr(expected)))
    elif isinstance(expected, int):
        if not isinstance(actual, int):
            raise AssertError('{0}: expecting int'.format(path))
        if actual != expected:
            raise AssertError('{0}: {1} (expecting {2})'.format(path, actual, expected))
    elif expected == None:
        if actual != None:
            raise AssertError('{0}: {1} (expecting None)'.format(path, repr(actual)))

def parse_json(actual, expected):
    variables = {}
    compare_json(actual, expected, variables)
    return variables

def add_to_json(js, path, value):
    add_to_json_rec(js, path.split('.'), value)

def add_to_json_rec(js, parts, value):
    if len(parts) == 1:
        js[parts[0]] = value
    else:
        if parts[0] not in js:
            js[parts[0]] = {}
        add_to_json_rec(js[parts[0]], parts[1:], value)

def extract_from_json(js, path):
    return extract_from_json_rec(js, path.split('.'))

def extract_from_json_rec(js, parts):
    if len(parts) == 0:
        return js
    if parts[0] in js:
        return extract_from_json_rec(js[parts[0]], parts[1:])
