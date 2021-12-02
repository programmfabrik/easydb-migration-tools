# coding=utf8

import sys
import json
import sqlite3
import hashlib
from datetime import datetime, timedelta
import urllib.request
import traceback
from six.moves.html_parser import HTMLParser


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
    print(timestamp, s)
    append_to_logfile(ERROR_LOG_FILE, s, timestamp)


def log_info(*strings):
    """
    log_error append values as new line to info log file
    """
    timestamp = datetime.now()
    s = format_string_list(strings)
    print(timestamp, s)
    append_to_logfile(INFO_LOG_FILE, s, timestamp)


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
        version = c.execute("""SELECT sqlite_version()""").fetchone()[0]
        log_info('Sqlite %s, Version: %s connected.' % (filename, version))
        return c
    except sqlite3.OperationalError as e:
        log_error('Error: Unable to open sqlite file: %s' % (filename))
        raise e


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
    return json.dumps(d, indent=indent, sort_keys=True)


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

    @classmethod
    def empty_payload(cls, import_type: str, obj_key: str = 'objects'):
        """
        empty_payload create an empty payload object

        :param import_type: value for import_type key
        :type import_type: str
        :param obj_key: key for array with objects, defaults to 'objects'
        :type obj_key: str, optional
        :return: payload object
        :rtype: dict
        """
        return {
            'import_type': import_type,
            obj_key: []
        }

    @classmethod
    def empty_db_payload(cls, objecttype: str):
        """
        empty_db_payload wrapper with empty payload with import_type 'db'

        :param objecttype: objecttype
        :type objecttype: str
        :return: payload object
        :rtype: dict
        """
        payload = cls.empty_payload('db')
        payload['objecttype'] = objecttype
        return payload

    def save_payloads(self, manifest: dict, outputfolder: str, objecttype: str, ref_col: str, batchsize: int):
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
        :return: manifest
        :rtype: dict
        """
        if not objecttype in self.export_objects:
            log_error(objecttype, 'not in export_objects')
            return

        all_objects = list(self.export_objects[objecttype].values())
        all_objects.sort(key=lambda x: x[objecttype][ref_col])

        offset = 0
        batch = 1
        has_more = True

        while has_more:

            objects = all_objects[offset:offset + batchsize]
            size = len(objects)
            if size < batchsize:
                has_more = False
            if size < 1:
                break

            filename = 'db__%s__batch_%04d__size_%04d.json' % (objecttype,
                                                               batch,
                                                               size)
            manifest = self.save_batch(
                objects,
                outputfolder + '/',
                filename,
                'db',
                manifest,
                objecttype)

            offset += batchsize
            batch += 1

        return manifest

    @classmethod
    def merge_object(cls, new_obj: dict, old_obj: dict, top_level: bool = True):
        """
        merge_object helper method to merge two objects with the same reference and different keys

        :param new_obj: object with new data
        :type new_obj: dict
        :param old_obj: old object
        :type old_obj: dict
        :param top_level: for recursive call of function, defaults to True
        :type top_level: bool, optional
        :raises Exception: Exception with information if merging failed
        :return: the merged object
        :rtype: dict
        """
        if top_level:
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

            old_obj[objecttype] = cls.merge_object(new_obj[objecttype],
                                                   old_obj[old_objecttype],
                                                   False)
            return old_obj

        obj = {}
        for obj_key in old_obj:
            obj[obj_key] = old_obj[obj_key]

        for obj_key in new_obj:
            new_v = new_obj[obj_key]

            if obj_key not in old_obj:
                obj[obj_key] = new_v
                continue

            old_v = old_obj[obj_key]

            if old_v is None:
                obj[obj_key] = new_v
                continue

            if isinstance(new_v, dict) and isinstance(old_v, dict):
                obj[obj_key] = cls.merge_object(new_v, old_v, False)
                continue

            if isinstance(new_v, list) and isinstance(old_v, list):
                result_array = []
                for o in old_v:
                    result_array.append(o)
                for o in new_v:
                    if o in result_array:
                        continue
                    result_array.append(o)

                obj[obj_key] = result_array
                continue

        return obj

    def merge_export_object(self, objecttype: str, ref_col: str, obj: dict):
        """
        merge_export_object add the object to the map of export object, merge if necessary

        :param objecttype: objecttype
        :type objecttype: str
        :param ref_col: name of the reference column, must be in the object
        :type ref_col: str
        :param obj: object with new data
        :type obj: dict
        :return: added number of objects: 1 of a new object was added, else 0
        :rtype: int
        """
        if not objecttype in obj:
            log_error(
                'merge_export_object: invalid object: given objecttype %s not in object' % (objecttype))
            return 0
        if not ref_col in obj[objecttype]:
            log_error('merge_export_object: invalid object: given reference field %s not in object.%s' % (
                ref_col, objecttype))
            return 0

        ref = obj[objecttype][ref_col]

        if objecttype not in self.export_objects:
            if self.verbose:
                log_info('add objecttype %s to export_objects' % (objecttype))
            self.export_objects[objecttype] = {}
        if ref not in self.export_objects[objecttype]:
            if self.verbose:
                log_info('insert new object of objecttype %s with ref %s' %
                         (objecttype, ref))
            self.export_objects[objecttype][ref] = obj
            return 1

        # object already exists, merge new object
        if self.verbose:
            log_info('update existing object of objecttype %s with ref %s' %
                     (objecttype, ref))
        old_obj = self.export_objects[objecttype][ref]
        self.export_objects[objecttype][ref] = self.merge_object(obj, old_obj)

        return 0

    @classmethod
    def save_batch(cls, payload: list, outputfolder: str, filename: str, import_type: str, manifest: dict, objecttype: str = None):
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
        :return: manifest
        :rtype: dict
        """
        data = {
            'import_type': import_type,
            import_type_array_map[import_type][0]: payload
        }
        if objecttype is not None:
            data['objecttype'] = objecttype

        f = open(outputfolder + filename, 'w')
        f.write(dumpjs(data))
        f.close()

        manifest['payloads'].append(filename)
        if objecttype is None:
            log_info('saved importtype', import_type, 'as', filename)
        else:
            log_info('saved objecttype', objecttype, 'as', filename)

        return manifest
