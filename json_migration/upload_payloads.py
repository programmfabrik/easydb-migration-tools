#!/usr/bin/python
# coding=utf8


import urllib3
import requests
import json
import argparse
from datetime import datetime
from . import migration_util

##################################
#                                #
#          easydb API            #
##################################


# server requests

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
    # print("compare_json("+str(type(actual))+" "+str(actual)+", "+str(type(expected))+" "+str(expected)+", "+str(type(variables))+" "+str(variables)+", "+str(type(path))+" "+str(path)+")"))
    if isinstance(actual, str):
        actual = str(actual)
    if isinstance(expected, str):
        expected = str(expected)
    if isinstance(path, str):
        path = str(path)

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
            print("ERROR:")
            print(r.text)
            raise Exception('Easydb returned an error')
        return json.loads(r.text)
    return new_f

class EasydbAPI(object):

    def __init__(self, url, auth=None):
        self.token = None
        self.url = url
        self.root_pool = None
        self.auth = auth

    def authenticate(self, login, password):
        print("authenticate as",login)
        session = self._get('session')
        token = parse_json(session, {'token':'$token'})['token']
        session_params = {
            'token': token,
            'login': login,
            'password': password
        }
        session = self._post('session/authenticate', params=session_params)
        self.token = parse_json(session, {'token':'$token', 'authenticated': {'method':'easydb'}})['token']
        print("easydb-token: " + self.token)

    def is_authenticated(self):
        return self.token is not None

    @authenticated
    def get(self, what, params={}):
        p = self.add_token_param(params)
        return self._get(what, p)

    @parsed
    def _get(self, what, params={}):
        # print('get {0} - {1}'.format(what, params))
        url = self.get_url(what)
        return requests.get(url, params=params, auth=self.auth)

    @authenticated
    def post(self, what, js=None, params={}, files=None):
        p = self.add_token_param(params)
        return self._post(what, js, p, files)

    @parsed
    def _post(self, what, js=None, params={}, files=None):
        data = js_body(js) if js is not None else None
        # print('post {0} - {1}:\n{2}'.format(what, params, data))
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
    return js
    #return json.dumps(js, indent=4)

def js_header(header={}):
    header['content-type'] = 'application/json'
    return header

def post_to_easydb(api, url, api_path, payload, login = "root", pw = "admin"):
    return request_to_easydb(api, url, api_path, payload, "POST", login, pw)

def put_to_easydb(api, url, api_path, payload, login = "root", pw = "admin"):
    return request_to_easydb(api, url, api_path, payload, "PUT", login, pw)

def request_to_easydb(api, url, api_path, payload = "", method = "GET", login = "root", pw = "admin"):
    try:
        print("API URL: " + url + "/api/v1/" + api_path)

        if method.upper() == "PUT":
            r = api.put(api_path, payload)
        elif method.upper() == "GET":
            r = api.get(api_path, {'format': 'standard'})
        elif method.upper() == "POST":
            r = api.post(api_path, payload)
        else:
            print(method,"NOT SUPPORTED")

        return r
    except Exception as e:
        print("REQUEST ERROR!\n  "+str(e))
        return None


def generate_payload(data):
    first = True
    s = "["
    for d in data:
        if first:
            first = False
        else:
            s += ","
        s += json.dumps(d)
    s += "]"
    return s


def upload(api_path, payload, serveraddress, method = "POST", login = "root", pw = "admin"):
    api = EasydbAPI(serveraddress)
    # print("PAYLOAD:",payload)
    api.authenticate(login, pw)
    if method == "POST":
        post_to_easydb(api, serveraddress, api_path, payload)
    elif method == "PUT":
        put_to_easydb(api, serveraddress, api_path, payload)
    else:
        "unknown method",method


###################################


argparser = argparse.ArgumentParser(description='Upload payloads to easydb 5')

argparser.add_argument('sourcefolder',                                    help='Base Folder for Payload files')
argparser.add_argument('serveraddress',                                   help='Server Address')
argparser.add_argument('-m',  '--manifest',      default='manifest.json', help='Name of manifest file (relative to sourcefolder), default manifest.json')
argparser.add_argument('-po', '--payloadoffset', default='0',             help='Offset of Payloads to upload, default 0')
argparser.add_argument('-pl', '--payloadlimit',  default='0',             help='Limit of Payloads to upload (limit <= 0 -> no limit), default 0')
argparser.add_argument('-l',  '--login',         default='root',          help='easydb root login (default: root)')
argparser.add_argument('-pw', '--password',      default='admin',         help='easydb root password (default: admin)')

args = argparser.parse_args()

path = args.sourcefolder
if path[-1] != '/':
    path += '/'

manifest = json.loads(open(path + args.manifest).read())
print("MANIFEST:",json.dumps(manifest, indent = 4))

batch_size = manifest['batch_size']

payloadoffset = int(args.payloadoffset)
if payloadoffset >= len(manifest["payloads"]):
    print("Payloadoffset must be <",len(manifest["payloads"]))
    exit()
if payloadoffset < 0:
    payloadoffset = 0

payloadlimit = int(args.payloadlimit)
if payloadlimit < 0:
    payloadlimit = 0

print("Payload Offset:",payloadoffset)
print("Payload Limit:",payloadlimit)

last_uploaded_payload = None
try:
    n = payloadoffset
    for p in manifest["payloads"][payloadoffset:]:

        batch_offset = 0

        payload = json.loads(open(path + p).read())

        print("[",n,"]")
        n += 1
        print("Payload from File",path + p)

        import_type = "objects"
        if "import_type" in payload:
            import_type = payload["import_type"]
        print("import_type:",import_type)
        payload = payload[migration_util.import_type_array_map[import_type][0]]

        api_path = None
        if import_type == "db":
            api_path = "db/" + payload[0]["_objecttype"]
        else:
            api_path = import_type + "/"

        print("API path:",api_path)

        if import_type in ["collection"]:
            # each object in array seperately
            for pl in payload:
                upload(
                    api_path,
                    json.dumps(pl, indent=2),
                    args.serveraddress,
                    migration_util.import_type_array_map[import_type][1],
					args.login,
					args.password
                )
        else:
            # complete array as one batch
            upload(
                api_path,
                generate_payload(payload),
                args.serveraddress,
                migration_util.import_type_array_map[import_type][1],
				args.login,
				args.password
            )

        if payloadlimit > 0:
            if n - payloadoffset >= payloadlimit:
                print("Payload Limit of",payloadlimit,"reached, stopping upload")
                break

    print("DONE, all payloads uploaded")

except KeyboardInterrupt as e:
    print("Upload was cancelled by user")
    if last_uploaded_payload is not None:
        print("last successfully uploaded payload:",last_uploaded_payload)
