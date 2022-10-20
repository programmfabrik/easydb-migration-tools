# coding=utf8

import sys
import os
import json
import sqlite3
import hashlib
from datetime import datetime, timedelta
import urllib.request
import traceback
from six.moves.html_parser import HTMLParser
import gzip
from inspect import currentframe, getframeinfo


import_type_array_map = {
    'tags': ('tags', 'POST'),
    'pool': ('pools', 'POST'),
    'user': ('users', 'POST'),
    'group': ('groups', 'POST'),
    'db': ('objects', 'POST'),
    'collection': ('collections', 'PUT')
}


# -----------------------------
# logging

ERROR_LOG_FILE = 'error.log'
INFO_LOG_FILE = 'info.log'


def append_to_logfile(logfile: str, s: str, timestamp: datetime = None):
    """
    append_to_logfile append line with timestamp to the logfile

    :param logfile: filename of logfile
    :type logfile: str
    :param s: line with log message
    :type s: str
    :param timestamp: timestamp, defaults to None, will then be set to datetime.now()
    :type timestamp: datetime, optional
    """
    if timestamp is None:
        timestamp = datetime.now()
    with open(logfile, 'a') as log:
        log.writelines([
            '\n',
            str(timestamp),
            '\t',
            s
        ])


def init_logfile(logfile: str):
    """
    init_logfile create empty logfile

    :param logfile: filename of logfile
    :type logfile: str
    """
    with open(logfile, 'w') as log:
        log.write('')


def init_error_log():
    """
    init_error_log call init_logfile for error log file
    """
    init_logfile(ERROR_LOG_FILE)


def init_info_log():
    """
    init_error_log call init_logfile for info log file
    """
    init_logfile(INFO_LOG_FILE)


def debugline():
    frame = currentframe()
    if frame is None:
        return ''
    frameinfo = getframeinfo(frame.f_back)
    return '%s:%d' % (
        os.path.basename(frameinfo.filename),
        frameinfo.lineno
    )


def format_string_list(strings):
    """
    format_string_list join values with spaces

    :param strings: list of values
    :type strings: list
    :return: string
    :rtype: string
    """
    string_list = []
    for s in strings:
        if isinstance(s, str):
            string_list.append(s)
        else:
            string_list.append(str(s))

    return ' '.join(string_list)


def log_error(*strings):
    """
    log_error append values as new line to error log file
    """
    timestamp = datetime.now()
    s = format_string_list(strings)
    append_to_logfile(ERROR_LOG_FILE, '[ERROR] ' + s, timestamp)


def log_info(*strings):
    """
    log_info append values as new line to info log file and print line to console
    """
    timestamp = datetime.now()
    s = format_string_list(strings)
    print(timestamp, s)
    append_to_logfile(INFO_LOG_FILE, '[INFO ] ' + s, timestamp)


def log_debug(*strings):
    """
    log_debug append values as new line to info log file
    """
    timestamp = datetime.now()
    s = format_string_list(strings)
    append_to_logfile(INFO_LOG_FILE, '[DEBUG] ' + s, timestamp)


def time_per_object(start: datetime, n: int):
    """
    time_per_object string with calculated average time to generate one object

    :param start: timestamp of start of generating objects
    :type start: datetime
    :param n: total number of objects
    :type n: int
    :return: formatted string
    :rtype: string
    """
    if n < 1:
        return '(0 ms per object)'
    diff = timedelta.total_seconds(datetime.now() - start)
    diff *= 1000.0
    return '(%.2f ms per object)' % (diff / n)


def percentage(n: int, total: int):
    """
    percentage string with calculated percentage between n and total

    :param n: number of objects
    :type n: int
    :param total: total number of objects
    :type total: int
    :return: formatted string
    :rtype: string
    """
    p = 0
    if total > 0 and n > 0:
        p = int((float(n) / float(total)) * 100.0)
    return '| %d / %d (%d%%)' % (n, total, p)


# -----------------------------
# convert and clean text with html tags

html_tags = {
    u'<br>': u'\n',
    u'<br/>': u'\n',
    u'<br />': u'\n',
}


def remove_xml_comments(v: str):
    """
    remove_xml_comments remove everything between xml comments (<!-- -->)

    :param v: text to clean
    :type v: str
    :return: cleaned text
    :rtype: str
    """
    com_start = '<!--'
    com_end = '-->'

    while com_start in v:
        res = ''
        ind1 = v.find(com_start)
        ind2 = v.find(com_end)

        if ind2 > ind1:
            res += v[:ind1]
            res += v[ind2 + len(com_end):]

        v = res

    return v


def clean_html(v: str, first: bool = True):
    """
    clean_html remove everything between html tags (< >), parse and replace html tags if possible

    :param v: text to clean
    :type v: str
    :param first: for multiple passes, is False in all later passes, defaults to True
    :type first: bool, optional
    :return: cleaned text
    :rtype: str
    """
    if v is None:
        return None
    if len(v) < 1:
        return None

    v = remove_xml_comments(v)

    for t in html_tags:
        v = v.replace(t, html_tags[t])

    text_between_brackets = []
    for s in v.split('<'):
        text_between_brackets.append(s.split('>')[-1])
    res = ''.join(text_between_brackets)

    if len(res) < 1:
        return None

    h = HTMLParser()
    res = h.unescape(res)

    if first:
        return clean_html(res, False)

    while '  ' in res:
        res = res.replace('  ', ' ')

    return res.strip()


# -----------------------------
# error handling


def print_traceback(e: Exception):
    """
    print_traceback print traceback of given exception

    :param e: Exception
    :type e: Exception
    :return: traceback
    :rtype: list of strings
    """
    exc_info = sys.exc_info()
    stack = traceback.extract_stack()
    tb = traceback.extract_tb(exc_info[2])
    full_tb = stack[:-1] + tb
    exc_line = traceback.format_exception_only(*exc_info[:2])
    traceback_array = [
        repr(e),
        traceback.format_list(full_tb) + exc_line
    ]

    traceback_str = '%s\n%s' % (
        traceback_array[0], ''.join(traceback_array[1]))

    log_error(traceback_str)

    return traceback_array


# -----------------------------
# sqlite3 functions


def sqlite3_connect(filename: str, detect_types: int = sqlite3.PARSE_DECLTYPES):
    """
    sqlite3_connect connect to sqlite3 file

    :param filename: sqlite3 file
    :type filename: str
    :param detect_types: detect types, defaults to sqlite3.PARSE_DECLTYPES
    :type detect_types: int, optional
    :raises e: sqlite3.OperationalError
    :return: connection
    :rtype: sqlite3.Connection
    """
    try:
        c = sqlite3.connect(filename, detect_types)
        version = sqlite3_execute(
            c,
            """
                SELECT sqlite_version()
            """
        )[0][0]
        log_info('Sqlite %s, Version: %s connected.' % (filename, version))
        return c
    except sqlite3.OperationalError as e:
        log_error('Error: Unable to open sqlite file: %s' % (filename))
        raise e


def sqlite3_execute(con: sqlite3.Connection, query: str, params=[], debug=False):
    """
    sqlite3_execute perform a SQL query

    :param con: connection to sqlite3 database
    :type con: sqlite3.Connection
    :param query: SQL query with statement
    :type query: str
    :param params: list of parameters, defaults to []
    :type params: list, optional
    :param debug: debug?, defaults to False
    :type debug: bool, optional
    :return: query result (if any)
    :rtype: list
    """
    if debug:
        log_info(query, '|', params)

    t1 = datetime.now()
    res = con.execute(query, params).fetchall()
    t2 = datetime.now()

    if debug:
        log_info('QUERY: %d rows (db: %d ms)' %
                 (len(res), (t2 - t1).microseconds / 1000))

    return res


def sqlite3_select(con: sqlite3.Connection, query: str, params=[], debug=False):
    """
    sqlite3_select perform a SELECT statement

    :param con: connection to sqlite3 database
    :type con: sqlite3.Connection
    :param query: SQL query with SELECT statement
    :type query: str
    :param params: list of parameters, defaults to []
    :type params: list, optional
    :param debug: debug?, defaults to False
    :type debug: bool, optional
    :return: list of rows, each row is dict with column names as keys
    :rtype: list
    """
    if debug:
        log_info(query, '|', params)

    t1 = datetime.now()
    cur = con.execute(query, params)

    columns = list(map(lambda x: x[0], cur.description))
    if debug:
        log_info('COLS: [%s]' % ','.join(columns))

    res = cur.fetchall()
    t2 = datetime.now()

    if len(res) < 1:
        return []

    result_rows = []
    for row in res:
        result_row = {}
        for i in range(len(columns)):
            result_row[columns[i]] = row[i]
        result_rows.append(result_row)

    t3 = datetime.now()

    if debug:
        log_info('SELECT: %d rows (db: %d ms, parse: %d ms)' % (len(res),
                                                                (t2 - t1).microseconds / 1000,
                                                                (t3 - t2).microseconds / 1000))

    return result_rows


def sqlite3_count_rows(con: sqlite3.Connection, table: str, specific_select: str = None, debug: bool = False):
    """
    sqlite3_count_rows count rows in a table, can be expanded to use a WHERE clause

    :param con: connection to sqlite3 database
    :type con: sqlite3.Connection
    :param table: table name
    :type table: str
    :param specific_select: WHERE clause that is used in the SELECT statement, defaults to None
    :type specific_select: str, optional
    :param debug: debug?, defaults to False
    :type debug: bool, optional
    :return: number of rows
    :rtype: int
    """

    if specific_select is not None:
        statement = 'SELECT count(*) AS n FROM ({0})'.format(specific_select)
    else:
        statement = 'SELECT count(*) AS n FROM "{0}"'.format(table)

    res = sqlite3_select(con, statement, [], debug=debug)
    if len(res) < 1:
        return None

    try:
        return int(res[0]['n'])
    except:
        return None


# -----------------------------
# helper methods

def generate_hash_reference(value):
    """
    generate_hash_reference convert value to md5 checksum

    :param value: value
    :type value: Any
    :return: string with md5 checksum in hex format
    :rtype: stri
    """
    return hashlib.md5(value.encode('utf-8')).hexdigest()


ISO_FORMAT_OUTPUT = '%Y-%m-%dT%H:%M:%S'
DATE_FORMAT_OUTPUT = '%Y-%m-%d'


class JsonWithSets(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, set):
            return list(o)
        return json.JSONEncoder.default(self, o)


def dumpjs(d: dict, indent=4):
    """
    dumpjs convert dict to a pretty printed json string

    :param d: dict
    :type d: dict
    :param indent: number of spaces for indent, defaults to 4
    :type indent: int, optional
    :return: pretty printed json string
    :rtype: str
    """
    return json.dumps(d, indent=indent, cls=JsonWithSets)


def datetime_to_iso(d: datetime):
    """
    datetime_to_iso format datetime object with easydb5 iso format '%Y-%m-%dT%H:%M:%S'

    :param d: datetime object
    :type d: datetime
    :return: formatted string
    :rtype: str
    """
    return d.strftime(ISO_FORMAT_OUTPUT)


def datetime_to_date(d: datetime):
    """
    datetime_to_iso format datetime object with easydb5 iso format '%Y-%m-%d'

    :param d: datetime object
    :type d: datetime
    :return: formatted string
    :rtype: str
    """
    return d.strftime(DATE_FORMAT_OUTPUT)


def to_easydb_date_object(d: str):
    """
    to_easydb_date_object wrapper for a easydb5 date object

    :param d: date string
    :type d: str
    :return: easydb5 date object
    :rtype: dict
    """

    if d is None:
        return None

    if len(d) < 1:
        return None

    return {
        'value': d
    }


def format_date_object(year: str, month: str, day: str):
    """
    format_date_object convert year, month, day strings to an easydb5 date object

    :param year: year
    :type year: str
    :param month: month, can be None
    :type month: str
    :param day: day, can be None
    :type day: str
    :return: easydb5 date object
    :rtype: dict
    """
    if year is None:
        return None

    n_year = None
    try:
        n_year = int(year)
    except:
        pass

    n_month = None
    try:
        n_month = int(month)
    except:
        pass

    n_day = None
    try:
        n_day = int(day)
    except:
        pass

    try:
        if n_month is None or n_month < 1 or n_month > 12:
            dstr = '%04d' % n_year
            datetime.strptime(dstr, '%Y')
            return to_easydb_date_object(dstr)

        if n_day is None or n_day < 1:
            dstr = '%04d-%02d' % (n_year, n_month)
            datetime.strptime(dstr, '%Y-%m')
            return to_easydb_date_object(dstr)

        dstr = '%04d-%02d-%02d' % (n_year, n_month, n_day)
        datetime.strptime(dstr, '%Y-%m-%d')
        return to_easydb_date_object(dstr)
    except:
        pass

    return None


def check_image_url_reachable(url: str, verbose: bool = False):
    """
    check_image_url_reachable check if the given URL is reachable

    :param url: URL
    :type url: str
    :param verbose: debug?, defaults to False
    :type verbose: bool, optional
    :return: if the URL is reachable
    :rtype: bool
    """
    try:
        request = urllib.request.Request(url)
        request.get_method = lambda: 'HEAD'
        response = urllib.request.urlopen(request)
        if response.getcode() == 200:
            if verbose:
                log_info('URL', url, 'reachable')
            return True
        else:
            log_error('URL', url, 'unreachable, Code:', response.getcode())
            if verbose:
                log_error(response.info())
            return False
    except Exception as e:
        log_error('URL', url, 'unreachable, Error:', e)
        return False


def save_json_to_gzip_file(outputfolder: str, filename: str, data: dict, compression: int) -> str:
    """
    save_json_to_gzip_file convert data to json string, save as compressed gzip file

    :param outputfolder: target folder for json files
    :type outputfolder: str
    :param filename: filename of json file
    :type filename: str
    :param data: data to convert to json
    :type data: dict
    :param compression: gzip compression (1-9)
    :type compression: int
    :return: renamed filename
    :rtype: str
    """

    # gzip compression
    if compression > 9:
        compression = 9

    filename += '.gz'
    with gzip.open('%s/%s' % (outputfolder, filename), 'wb', compresslevel=compression) as f:
        f.write(dumpjs(data).encode('utf-8'))

    return filename

# -----------------------------


class ObjectPayloadManager(object):

    def __init__(self, verbose: bool) -> None:
        """
        __init__ Constructor

        :param verbose: debug?
        :type verbose: bool
        """
        super().__init__()

        self.verbose = verbose
        self.export_objects = {}
        self.hierarchies = {}

    @classmethod
    def empty_payload(cls, import_type: str):
        """
        empty_payload create an empty payload object

        :param import_type: value for import_type key
        :type import_type: str
        :raises Exception: Exception if the import_type is unknown
        :return: payload object and key of the array with objects
        :rtype: dict, str
        """

        if not import_type in import_type_array_map:
            raise Exception('unknown import_type %s' % (import_type))

        obj_key = import_type_array_map[import_type][0]

        return {
            'import_type': import_type,
            obj_key: []
        }, obj_key

    @classmethod
    def empty_db_payload(cls, objecttype: str):
        """
        empty_db_payload wrapper with empty payload with import_type 'db'

        :param objecttype: objecttype
        :type objecttype: str
        :return: payload object and key of the array with objects
        :rtype: dict, str
        """
        payload, obj_key = cls.empty_payload('db')
        payload['objecttype'] = objecttype
        return payload, obj_key

    @classmethod
    def version_invalid(cls, obj, objecttype, version, ref_col):
        if version == 0:
            return False
        if not '_version' in obj[objecttype]:
            log_error(obj[objecttype][ref_col], '_version not set -> skip')
            return True
        if obj[objecttype]['_version'] != version:
            log_error(obj[objecttype][ref_col], '_version =', obj[objecttype]
                      ['_version'], '!= ', version)
            # do not check version if the version is 0 in combination with auto_increment (only works for fylr!)
            if obj[objecttype]['_version'] != 0:
                log_error(obj[objecttype][ref_col], '_version != 0 -> skip')
                return True
            if not '_version:auto_increment' in obj[objecttype]:
                log_error(
                    obj[objecttype][ref_col], '_version = 0 but _version:auto_increment not set -> skip')
                return True
            if not obj[objecttype]['_version:auto_increment']:
                log_error(
                    obj[objecttype][ref_col], '_version = 0 but _version:auto_increment not TRUE -> skip')
                return True

        return False

    def save_payloads(self, manifest: dict, outputfolder: str, objecttype: str, ref_col: str, batchsize: int, refs: list = [], batchnumber: int = 0, is_hierarchical: bool = False, version: int = 1, compression: int = 0):
        """
        save_payloads save objects as json files, add payload names to manifest

        :param manifest: manifest
        :type manifest: dict
        :param outputfolder: target folder for json files
        :type outputfolder: str
        :param objecttype: objecttype
        :type objecttype: str
        :param ref_col: name of the reference column, must be in the object
        :type ref_col: str
        :param batchsize: maximal number of objects per json file
        :type batchsize: int
        :param refs: only export these refs if this array is not empty, defaults to []
        :type refs: list, optional
        :param batchnumber: optional number of the current batch, in case the objects have to be deleted between batches for performance reasons, defaults to 0. If it is <1, this value will be ignored. This will not work for hierarchical objects
        :type batchnumber: int, optional
        :param is_hierarchical: objecttype is hierarchical, defaults to False
        :type is_hierarchical: bool, optional
        :param version: version of exported objects, defaults to 1. is ignored for checks if it is 0
        :type version: int, optional
        :param compression: gzip compression (1-9), defaults to 0. 0 means no compression
        :type compression: int, optional
        :return: manifest
        :rtype: dict
        """
        if not objecttype in self.export_objects:
            log_error(objecttype, 'not in export_objects')
            return

        if is_hierarchical:
            return self.save_hierarchical_payloads(
                manifest,
                outputfolder,
                objecttype,
                ref_col,
                batchsize,
                refs,
                version,
                compression=compression)

        all_objects = []
        for obj in self.export_objects[objecttype].values():
            if not objecttype in obj:
                continue

            if len(refs) > 0:
                if not ref_col in obj[objecttype]:
                    continue
                if obj[objecttype][ref_col] not in refs:
                    continue

            if self.version_invalid(obj, objecttype, version, ref_col):
                continue

            all_objects.append(obj)

        log_info('save', len(all_objects), 'objects of type', objecttype)

        offset = 0
        batch = 1
        has_more = len(all_objects) > 0

        while has_more:

            objects = all_objects[offset:offset + batchsize]
            size = len(objects)
            if size < batchsize:
                has_more = False
            if size < 1:
                break

            filename = 'db__v%d__%s__batch_%04d__size_%04d.json' % (version,
                                                                    objecttype,
                                                                    batch if batchnumber < 1 else batchnumber,
                                                                    size)

            manifest = self.save_batch(
                objects,
                outputfolder + '/',
                filename,
                'db',
                manifest,
                objecttype,
                compression=compression)

            offset += batchsize
            batch += 1

        return manifest

    def save_hierarchical_payloads(self, manifest: dict, outputfolder: str, objecttype: str, ref_col: str, batchsize: int, refs: list = [], version: int = 1, compression: int = 0):
        """
        save_hierarchical_payloads save objects as json files, add payload names to manifest

        :param manifest: manifest
        :type manifest: dict
        :param outputfolder: target folder for json files
        :type outputfolder: str
        :param objecttype: objecttype
        :type objecttype: str
        :param ref_col: name of the reference column, must be in the object
        :type ref_col: str
        :param batchsize: maximal number of objects per json file
        :type batchsize: int
        :param refs: only export these refs if this array is not empty, defaults to []
        :type refs: list, optional
        :param version: version of exported objects, defaults to 1
        :type version: int, optional
        :param compression: gzip compression (1-9), defaults to 0. 0 means no compression
        :type compression: int, optional
        :return: manifest
        :rtype: dict
        """

        if not objecttype in self.hierarchies:
            log_error(objecttype, 'not in hierarchies')
            return manifest

        objects_tree = self.group_tree_by_levels(self.hierarchies[objecttype])

        for level in objects_tree:

            all_objects = []
            for ref in objects_tree[level]:
                if ref not in self.export_objects[objecttype]:
                    continue

                obj = self.export_objects[objecttype][ref]
                if not objecttype in obj:
                    continue

                if len(refs) > 0:
                    if not ref_col in obj[objecttype]:
                        continue
                    if obj[objecttype][ref_col] not in refs:
                        continue

                if self.version_invalid(obj, objecttype, version, ref_col):
                    continue

                all_objects.append(obj)

            offset = 0
            batch = 1
            has_more = len(all_objects) > 0

            while has_more:

                objects = all_objects[offset:offset + batchsize]
                size = len(objects)
                if size < batchsize:
                    has_more = False
                if size < 1:
                    break

                filename = 'db__v%d__%s__level_%02d__batch_%04d__size_%04d.json' % (version,
                                                                                    objecttype,
                                                                                    int(level),
                                                                                    batch,
                                                                                    size)
                manifest = self.save_batch(
                    objects,
                    outputfolder + '/',
                    filename,
                    'db',
                    manifest,
                    objecttype,
                    compression=compression)

                offset += batchsize
                batch += 1

        return manifest

    @classmethod
    def group_tree_by_levels(cls, hierarchie_list: dict, parent: str = None, tree_levels: dict = {}, level: int = 0):
        """
        group_tree_by_levels iterate recursively over the hierarchy to group the references by level

        :param hierarchie_list: list of references and their child references
        :type hierarchie_list: dict
        :param parent: parent of the current references, defaults to None
        :type parent: str, optional
        :param tree_levels: generated map of levels, defaults to {}
        :type tree_levels: dict, optional
        :param level: level (depth) in the hierarchie, defaults to 0
        :type level: int, optional
        :return: generated map of levels and references
        :rtype: dict
        """

        if parent not in hierarchie_list:
            if parent is None:
                log_error('no top level objects for hierarchical objecttype')
            return {}

        level_key = str(level)
        if not level_key in tree_levels:
            tree_levels[level_key] = []

        for leaf in hierarchie_list[parent]:
            if leaf in tree_levels[level_key]:
                continue

            tree_levels[level_key].append(leaf)

            cls.group_tree_by_levels(
                hierarchie_list,
                parent=leaf,
                tree_levels=tree_levels,
                level=level + 1
            )

        return tree_levels

    @classmethod
    def merge_object(cls, ref, new_obj: dict, old_obj: dict, path: list = []):
        """
        merge_object helper method to merge two objects with the same reference and different keys

        :param ref: object reference (for debugging)
        :type ref: str
        :param new_obj: object with new data
        :type new_obj: dict
        :param old_obj: old object
        :type old_obj: dict
        :param path: for recursive call of function, defaults to [] (for debugging)
        :type path: list, optional
        :raises Exception: Exception with information if merging failed
        :return: the merged object and if the object has been changed
        :rtype: dict, bool
        """
        if len(path) == 0:
            if not '_objecttype' in new_obj:
                raise Exception(
                    'could not merge objects: _objecttype missing in new obj')
            if not '_objecttype' in old_obj:
                raise Exception(
                    'could not merge objects: _objecttype missing in old obj')

            objecttype = new_obj['_objecttype']
            old_objecttype = old_obj['_objecttype']
            if objecttype != old_objecttype:
                raise Exception(
                    'could not merge objects: objecttypes do not match')

            if not objecttype in new_obj:
                raise Exception(
                    'could not merge objects: %s missing in new obj' % objecttype)
            if not old_objecttype in old_obj:
                raise Exception(
                    'could not merge objects: %s missing in old obj' % old_objecttype)

            old_obj[objecttype], updated = cls.merge_object(ref,
                                                            new_obj[objecttype],
                                                            old_obj[old_objecttype],
                                                            path=[objecttype])
            return old_obj, updated

        updated = False
        obj = {}
        for obj_key in old_obj:
            obj[obj_key] = old_obj[obj_key]

        for obj_key in new_obj:
            new_v = new_obj[obj_key]
            if new_v is None:
                continue

            if obj_key in ['_version', '_version:auto_increment', 'lookup:_id']:
                # if only the version or id lookup was changed/added, the object is not considered as updated
                obj[obj_key] = new_v
                continue

            if obj_key not in old_obj:
                obj[obj_key] = new_v
                updated = True
                continue

            old_v = old_obj[obj_key]

            if isinstance(new_v, dict) and isinstance(old_v, dict):
                obj[obj_key], _updated = cls.merge_object(
                    ref,
                    new_v,
                    old_v,
                    path=path + [obj_key])
                if _updated:
                    updated = True
                continue

            if isinstance(new_v, list) and isinstance(old_v, list):
                result_array = []
                for o in old_v:
                    if o in result_array:
                        continue
                    result_array.append(o)
                for o in new_v:
                    if o in result_array:
                        continue
                    result_array.append(o)
                    updated = True

                obj[obj_key] = result_array
                continue

            if new_v != old_v:
                obj[obj_key] = new_v
                updated = True

        return obj, updated

    def merge_export_object(self, objecttype: str, ref_col: str, obj: dict, parent_key: str = None):
        """
        merge_export_object add the object to the map of export object, merge if necessary

        :param objecttype: objecttype
        :type objecttype: str
        :param ref_col: name of the reference column, must be in the object
        :type ref_col: str
        :param obj: object with new data
        :type obj: dict
        :param parent_key: key for the parent relation if objecttype is hierarchical, defaults to None
        :type parent_key: str, optional
        :return: added number of objects: 1 of a new object was added, else 0
        :rtype: int
        """
        if not objecttype in obj:
            log_error(
                'merge_export_object: invalid object: given objecttype %s not in object' % (objecttype))
            return 0, False
        if not ref_col in obj[objecttype]:
            log_error('merge_export_object: invalid object: given reference field %s not in object.%s' % (
                ref_col, objecttype))
            return 0, False

        ref = obj[objecttype][ref_col]
        if ref is None:
            log_error('merge_export_object: invalid object: given reference field %s in object.%s is None' % (
                ref_col, objecttype))
            return 0, False

        if parent_key is not None:
            # insert the reference of the hierarchical object into the tree structure
            if not self.insert_object_ref_into_hierarchie(obj, objecttype, ref, ref_col, parent_key):
                return 0, False

        if objecttype not in self.export_objects:
            if self.verbose:
                log_info('add objecttype %s to export_objects' % (objecttype))
            self.export_objects[objecttype] = {}
        if ref not in self.export_objects[objecttype]:
            if self.verbose:
                log_info('insert new object of objecttype %s with ref %s' %
                         (objecttype, ref))
            self.export_objects[objecttype][ref] = obj
            return 1, True

        # object already exists, merge new object
        if self.verbose:
            log_info('update existing object of objecttype %s with ref %s' %
                     (objecttype, ref))

        old_obj = self.export_objects[objecttype][ref]
        new_obj, updated = self.merge_object(
            ref,
            obj,
            old_obj)
        self.export_objects[objecttype][ref] = new_obj

        return 0, updated

    def insert_object_ref_into_hierarchie(self, obj: dict, objecttype: str, ref: str, ref_col: str, parent_key: str):
        if not objecttype in obj:
            return False

        parent_ref = None
        if parent_key in obj[objecttype]:
            if ref_col in obj[objecttype][parent_key]:
                parent_ref = obj[objecttype][parent_key][ref_col]

        if not objecttype in self.hierarchies:
            self.hierarchies[objecttype] = {}

        if not parent_ref in self.hierarchies[objecttype]:
            self.hierarchies[objecttype][parent_ref] = {}

        self.hierarchies[objecttype][parent_ref][ref] = None

        return True

    def export_object_exists(self, objecttype: str, ref: str):
        """
        export_object_exists check if an object exists in the map of objects that will be exported

        :param objecttype: objecttype of the object
        :type objecttype: str
        :param ref: reference of the object
        :type ref: str
        :return: True if the object exists, else False
        :rtype: bool
        """
        if not objecttype in self.export_objects:
            return False
        return ref in self.export_objects[objecttype]

    def get_export_object(self, objecttype: str, ref: str):
        """
        get_export_object returns an object if it exists in the map of objects that will be exported

        :param objecttype: objecttype of the object
        :type objecttype: str
        :param ref: reference of the object
        :type ref: str
        :return: object if it exists, else None
        :rtype: dict
        """
        if not self.export_object_exists(objecttype, ref):
            return None

        return self.export_objects[objecttype][ref]

    def get_export_object_references(self, objecttype: str):
        """
        get_export_object_references returns a list of all references for the objecttype if there are any

        :param objecttype: objecttype of the object
        :type objecttype: str
        :return: list of all references, empty list if there are none
        :rtype: list
        """
        if not objecttype in self.export_objects:
            return []
        return list(self.export_objects[objecttype].keys())

    def update_object(self, objecttype, ref, obj):
        """
        update_object updates the object with the given ref with the given object (if it exists)

        :param objecttype: objecttype of the object
        :type objecttype: str
        :param ref: reference of the object
        :type ref: str
        :param obj: object the replaces the existing object
        :type obj: dict
        """

        self.export_objects[objecttype][ref] = obj

    def delete_object(self, objecttype, ref):
        """
        delete_object deletes the object with the given ref with the given objecttype (if it exists)

        :param objecttype: objecttype of the object
        :type objecttype: str
        :param ref: reference of the object
        :type ref: str
        """

        if self.export_object_exists(objecttype, ref):
            del self.export_objects[objecttype][ref]

    def delete_objects_by_objecttype(self, objecttype):
        """
        delete_objects_by_objecttype deletes all objects of the given objecttype if any exist

        :param objecttype: objecttype of the objects to delete
        :type objecttype: str
        """

        if objecttype in self.export_objects:
            self.export_objects[objecttype] = {}

    @classmethod
    def save_batch(cls, payload: list, outputfolder: str, filename: str, import_type: str, manifest: dict, objecttype: str = None, compression: int = 0):
        """
        save_batch save the batch of objects/basetypes as json files

        :param payload: payload
        :type payload: list
        :param outputfolder: target folder for json files
        :type outputfolder: str
        :param filename: filename of json file
        :type filename: str
        :param import_type: value for import_type key
        :type import_type: str
        :param manifest: manifest
        :type manifest: dict
        :param objecttype: objecttype if the payload does not contain basetypes, defaults to None
        :type objecttype: str, optional
        :param compression: gzip compression (1-9), defaults to 0. 0 means no compression
        :type compression: int, optional
        :return: manifest
        :rtype: dict
        """
        data = {
            'import_type': import_type,
            import_type_array_map[import_type][0]: payload
        }
        if objecttype is not None:
            data['objecttype'] = objecttype

        if compression < 1:
            with open('%s/%s' % (outputfolder, filename), 'w') as f:
                f.write(dumpjs(data))
        else:
            filename = save_json_to_gzip_file(
                outputfolder, filename, data, compression)

        manifest['payloads'].append(filename)
        if objecttype is None:
            log_info('saved importtype', import_type, 'as', filename)
        else:
            log_info('saved objecttype', objecttype, 'as', filename)

        return manifest
