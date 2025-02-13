'''
* easydb-migration-tools - easydb Migration Tools
 * Copyright (c) 2013 - 2016 Programmfabrik GmbH
 * MIT Licence
 * https://github.com/programmfabrik/coffeescript-ui, http://www.coffeescript-ui.org
'''

import sqlite3
import os
import sys
import decimal
import json
import chardet
import datetime

source_conn = None
global args
global silent

# we excect args to be a map, it can contain "silent"
# to surpress some of the output

try:
    silent = args.silent
except NameError:
    silent = False


def sqlite_init():
    def adapt_list(l):
        return json.dumps(l)

    def adapt_dict(d):
        return json.dumps(d)

    def adapt_decimal(d):
        return str(d)

    def convert_decimal(s):
        return decimal.Decimal(s)

    sqlite3.register_adapter(list, adapt_list)
    sqlite3.register_converter("list", json.loads)

    sqlite3.register_adapter(dict, adapt_dict)
    sqlite3.register_converter("dict", json.loads)

    sqlite3.register_adapter(decimal.Decimal, adapt_decimal)
    sqlite3.register_converter("decimal", convert_decimal)


def __str_to_unicode(s):
    if isinstance(s, str):
        return s
    try:
        return s.decode("utf-8")
    except (UnicodeDecodeError, UnicodeEncodeError) as e:
        # automatic detection fails sometimes, so we try latin1 first
        try:
            return s.decode("latin1")
        except (UnicodeDecodeError, UnicodeEncodeError) as e:
            try:
                encoding = chardet.detect(s)['encoding']
                return s.decode(encoding)
            except Exception as e:
                print()
                print("Error: __str_to_unicode: ", repr(s), "Error:", e)
                raise e
        # print(rec[cn])


def __execute(cursor, sql, bindings=None):
    global silent

    try:
        if bindings != None:
            return cursor.execute(sql, bindings)
        else:
            return cursor.execute(sql)
    except Exception as e:
        print(sql, end=' ')
        if bindings:
            print(bindings)
        else:
            print()

        raise e


def commit_source():
    global source_conn
    if source_conn:
        source_conn.commit()
        source_conn.close()
        print("Notice: Commited source.")


# source is the output file of this script, the name source stems from the following step in the migration
def prepare_source(
    # filename for the sqlite database file, where the results will be written into
    source="result.sqlite",
    init=True,  # set True and source will be purged
    init_filestore=None,  # create crucial tables inside the target sqlite file
):

    global source_conn

    if init_filestore == None:
        if init == False:
            init_filestore = False
        else:
            init_filestore = True

    try:
        if source_conn:
            commit_source()
    except NameError:
        pass

    filename = __str_to_unicode(source)

    print("""Preparing source "%s" """ % filename, file=sys.stderr)

    if init and init_filestore:
        try:
            os.remove(filename)
            print("Old database removed:", filename)
        except:
            pass

    # FIXME: if Init==False: check for file existence!

    try:
        source_conn = sqlite3.connect(filename, detect_types=sqlite3.PARSE_DECLTYPES)
    except sqlite3.OperationalError as e:
        print("Error: Unable to open sqlite file: " + filename)
        raise e

    source_conn.execute(
        """
        PRAGMA foreign_keys=ON
    """
    )

    version = source_conn.execute(
        """
        SELECT sqlite_version()
    """
    ).fetchone()[0]
    print("""Sqlite %s, Version: %s connected.""" % (filename, version))

    if init:
        if not init_filestore:
            sql = source_conn.cursor()
            row = sql.execute(
                """
                SELECT count(filestore_id), sum(filesize)
                FROM filestore
            """
            ).fetchone()

            print(
                """Keeping filestore in source "%s", removing all origins. \nFilestore currently holds %s files in %s bytes."""
                % (source, row[0], row[1])
            )

            # remove manually
            sql.execute(
                """
                SELECT DISTINCT(source_name)
                FROM origin
            """
            )
            for row in sql.fetchall():
                remove_from_source(row[0])
            sql.close()
        else:
            source_conn.execute(
                """
                CREATE TABLE origin (
                    origin_id INTEGER PRIMARY KEY,
                    origin_type TEXT NOT NULL,
                    origin_database_name TEXT NOT NULL,
                    origin_table_name TEXT NOT NULL,
                    source_name TEXT NOT NULL,
                    source_table_name TEXT NOT NULL,
                    UNIQUE(origin_type, origin_database_name, origin_table_name),
                    UNIQUE(source_name, source_table_name)
                )
            """
            )

            # for XML

            source_conn.execute(
                """
                CREATE TABLE IF NOT EXISTS xmldata (
                    node_id_path TEXT NOT NULL,
                    node_element_path TEXT NOT NULL,
                    node_id INTEGER PRIMARY KEY,
                    node_parent_id INTEGER,
                    node_level INTEGER NOT NULL,
                    node_element TEXT,
                    node_attrs TEXT,
                    node_data TEXT
                )
            """
            )

            __execute(
                source_conn,
                """
                CREATE INDEX xmldata_idx ON xmldata (node_id_path)
            """,
            )

            source_conn.execute(
                """
                CREATE TABLE IF NOT EXISTS "xmldata.transcribed" (
                    node_id_path TEXT NOT NULL,
                    node_element_path TEXT NOT NULL
                )
            """
            )

            # K10plus

            # table that stores values which belong to items
            source_conn.execute(
                """
                CREATE TABLE IF NOT EXISTS k10plus_source (
                    filename TEXT NOT NULL,
                    offset INTEGER NOT NULL,
                    from_item_id INTEGER,
                    to_item_id INTEGER
                )
            """
            )

            # table that stores values which belong to items (pica feld and unterfeld in rows)
            source_conn.execute(
                """
                CREATE TABLE IF NOT EXISTS k10plus_data (
                    id INTEGER PRIMARY KEY,
                    item_id INTEGER NOT NULL,
                    feld TEXT NOT NULL,
                    unterfeld TEXT NOT NULL,
                    wert TEXT NOT NULL
                )
            """
            )

        print("Notice: Create Base Tables Sqlite Database \"%s\"" % filename)
    else:
        print("Using existing Sqlite Database: \"%s\"" % filename)

    if init_filestore:
        if not init:
            print("Notice: Dropping Filestore Tables", filename)
            source_conn.execute(
                """
                PRAGMA foreign_keys=OFF
            """
            )
            __execute(
                source_conn,
                """
                DROP TABLE IF EXISTS "file"
            """,
            )
            __execute(
                source_conn,
                """
                DROP TABLE IF EXISTS "filestore"
            """,
            )
            source_conn.execute(
                """
                PRAGMA foreign_keys=ON
            """
            )

        __execute(
            source_conn,
            """
            CREATE TABLE file (
                file_id INTEGER PRIMARY KEY,
                filestore_id INTEGER NOT NULL,

                source_name TEXT NOT NULL,
                source_table_name TEXT NOT NULL,
                source_column_name TEXT NOT NULL,
                source_unique_id TEXT NOT NULL,

                origin_url TEXT,
                eas_id INTEGER,
                eas_root_id INTEGER,
                file_version TEXT,

                UNIQUE(eas_id, file_version),
                FOREIGN KEY (filestore_id) REFERENCES filestore(filestore_id) ON DELETE RESTRICT
            )
        """,
        )

        __execute(
            source_conn,
            """
            CREATE INDEX file_idx ON file (
                source_name,
                source_table_name,
                source_column_name,
                source_unique_id
            )
        """,
        )

        __execute(
            source_conn,
            """
            CREATE TABLE filestore (
                filestore_id INTEGER PRIMARY KEY,
                unique_id TEXT NOT NULL UNIQUE,
                original_filename TEXT,
                mimetype TEXT,
                filesize INTEGER,
                url TEXT,
                filename TEXT,
                data BLOB
            )
        """,
        )

        print("Notice: Created Filestore Tables:", filename)


# remove all data from the source for a
# specific name


def remove_from_source(
    name,  # name of source
    keep_filestore=False,  # whether to keep or drop files from filestore
):
    global source_conn

    print("\nRemoving all tables for source", name, ".", end=' ')
    if keep_filestore:
        print("Keeping files in filestore...")
    else:
        print("Removing matching files from filestore...")

    sql = source_conn.cursor()
    __execute(
        sql,
        """
        SELECT source_name, source_table_name
        FROM origin
        WHERE source_name=?
    """,
        [name],
    )
    for row in sql.fetchall():
        tntpl = (row[0], row[1])
        __execute(
            sql,
            """
            DROP TABLE IF EXISTS "%s.%s"
        """
            % tntpl,
        )

    if not keep_filestore:
        __execute(
            sql,
            """
            DELETE FROM filestore
            WHERE filestore_id IN (
                SELECT filestore_id
                FROM file
                WHERE source_name=?
            )
        """,
            [name],
        )
        __execute(
            sql,
            """
            DELETE FROM file
            WHERE source_name=?
        """,
            [name],
        )

    __execute(
        sql,
        """
        DELETE FROM origin
        WHERE source_name=?
    """,
        [name],
    )


# K10+


DELIMITER_OBJ = '\x1d'  # Group separator (Information Separator Three)
DELIMITER_REC = '\x1e'  # Record separator (Information Separator Two)
DELIMITER_VAL = '\x1f'  # Unit separator (Information Separator One)
DELIMITER_FALLBACK = '$'
NEWLINE = '\n'

KNOWN_SUB_FIELDS = [
    '0',
    '2',
    '3',
    '4',
    '5',
    '6',
    '7',
    '8',
    '9',
    'a',
    'A',
    'b',
    'B',
    'c',
    'C',
    'd',
    'e',
    'f',
    'g',
    'h',
    'i',
    'j',
    'k',
    'l',
    'L',
    'm',
    'n',
    'N',
    'o',
    'p',
    'P',
    'q',
    'r',
    'S',
    's',
    't',
    'T',
    'U',
    'u',
    'v',
    'w',
    'X',
    'x',
    'y',
    'z',
]


def parse_line(line):

    if line[0] == DELIMITER_OBJ:
        return [DELIMITER_OBJ]

    if line[0] == DELIMITER_REC:
        line = line[1:]
    else:
        return []

    while line[-1] == NEWLINE:
        line = line[:-1]

    res = []
    part = ''
    i = 0
    for c in line:
        if c == DELIMITER_VAL:
            if part != '':
                res.append(part.strip())
                part = ''
                i += 1
                continue

        if c == DELIMITER_FALLBACK:
            if i < len(line) - 1:
                if line[i + 1] in KNOWN_SUB_FIELDS:
                    res.append(part.strip())
                    part = ''
                    i += 1
                    continue

        i += 1
        part += c

    if part != '':
        res.append(part.strip())

    return res


def group_line(parsed):
    if len(parsed) < 2:
        return None

    res = {}
    for i in range(len(parsed) - 1):
        p = parsed[i + 1]
        if len(p) < 2:
            continue

        if p[0] not in res:
            res[p[0]] = []

        res[p[0]].append(p[1:])

    return res


def k10plus_get_offset(filename):

    global source_conn
    cur = source_conn.cursor()

    __execute(
        cur,
        """
        SELECT max(to_item_id)
        FROM k10plus_source

        UNION ALL

        SELECT offset
        FROM k10plus_source
        WHERE filename = '{source}'
    """.format(
            source=os.path.basename(filename)
        ),
    )
    rows = cur.fetchall()
    cur.close()

    item_id_offset = 0
    offset = 0

    if rows is None:
        return item_id_offset, offset

    if len(rows) > 0:
        if rows[0][0] is not None:
            item_id_offset = int(rows[0][0])
    if len(rows) > 1:
        if rows[1][0] is not None:
            offset = int(rows[1][0])

    return item_id_offset, offset


def k10plus_to_source(
    filename,  # .pp filename
    item_id_offset,  # last assigned item_id to start of when assigning new ids for new items
    offset,  # offset in the current file if items where already saved from this file
):
    global source_conn
    cur = source_conn.cursor()

    basename = os.path.basename(filename)

    item_id = item_id_offset

    start = datetime.datetime.now()
    total_items = 0
    total_rows = 0

    cur_item = 0
    skip_current_object = False

    interrupt = False
    close_connection = False

    # if the file was not read yet (partially), add it to the source table
    if offset == 0:
        # insert a new entry in the source table
        __execute(
            cur,
            """
            INSERT INTO k10plus_source
            (filename, offset, from_item_id)
            VALUES (?,?,?)
        """,
            (basename, 0, item_id_offset + 1),
        )

    try:

        with open(filename, 'r') as f:
            for line in f:

                parsed = parse_line(line)
                if len(parsed) == 0:
                    continue

                feld = parsed[0]

                # skip all lines if the current object is to be ignored
                if skip_current_object and feld != DELIMITER_OBJ:
                    continue

                # new entry
                if feld == DELIMITER_OBJ:

                    skip_current_object = False

                    # skip the object if it was imported already
                    if offset > 0 and cur_item <= offset:

                        skip_current_object = True
                        cur_item += 1

                        continue

                    item_id += 1
                    cur_item += 1
                    total_items += 1

                    if item_id % 1000 == 0:
                        print('\r' + filename + '\t' + str(item_id), end=' ')
                        sys.stdout.flush()

                    continue

                if len(parsed) < 2:
                    continue

                group = group_line(parsed)
                for unterfeld in group:
                    for wert in group[unterfeld]:
                        # insert a new entry in the data table, add an uplink to the item
                        __execute(
                            cur,
                            """
                            INSERT INTO k10plus_data
                            (item_id, feld, unterfeld, wert)
                            VALUES (?,?,?,?)
                        """,
                            (item_id, feld, unterfeld, wert),
                        )
                        total_rows += 1

    except KeyboardInterrupt:
        interrupt = True
        close_connection = True

    except Exception as e:
        close_connection = True
        raise e

    # update offset and last item id in source table
    __execute(
        cur,
        """
        UPDATE k10plus_source
        SET offset = ?, to_item_id = ?
        WHERE filename = ?
    """,
        (cur_item - 1, item_id, basename),
    )

    cur.close()
    if close_connection:
        source_conn.commit()

    if item_id > 1000:
        print()
    print(
        filename,
        '| duration:',
        (datetime.datetime.now() - start),
        '|',
        total_items,
        'items,',
        total_rows,
        'rows',
    )

    if interrupt:
        exit(0)

    return item_id
