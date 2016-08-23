import sqlite3
import psycopg2
import psycopg2.extensions
import os
import sys
import decimal
import json
import MySQLdb
import requests
import chardet
import csv
import xml.parsers.expat

source_conn = None

def __pg_init():
    psycopg2.extensions.register_type(psycopg2.extensions.UNICODE)
    psycopg2.extensions.register_type(psycopg2.extensions.UNICODEARRAY)

def __sqlite_init():
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

def __str_to_unicode (s):
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
                print
                print "Error: __str_to_unicode: ", repr(s), "Error:", e
                raise e
        # print rec[cn]

def __execute(cursor, sql, bindings=None):
    global args

    try:
        if bindings != None:
            return cursor.execute(sql, bindings)
        else:
            return cursor.execute(sql)
    except Exception as e:
        print sql,
        if bindings:
            print bindings
        else:
            print

        raise e

def __commit_source ():
    global source_conn
    if source_conn:
        source_conn.commit()
        source_conn.close()
        print "Notice: Commited source."

def __pg_get_schema(conn,
                    schema_name=None,
                    include_tables=None,
                    include_tables_exclusive=True,
                    include_schema_in_table_name=True,
                    exclude_tables=None):

    cur = conn.cursor()
    cur.execute("SELECT current_database()")
    db_name = cur.fetchall()[0][0]

    schema = {
        "tables": {},
        "type": "postgresql",
        "database": db_name
        }

    if schema_name != None:
        schema_filter = " AND table_schema = '"+schema_name+"'"
    else:
        schema_filter = ""

    cur.execute("""
    SELECT
      table_schema, table_name, column_name, data_type

    FROM
      information_schema.columns

    WHERE
      table_schema not in ('pg_catalog','information_schema') AND
      data_type NOT IN ('tsvector') AND
      table_name not in ('easydb_autocomplete')
      %s

    ORDER BY table_name, ordinal_position
    """ % schema_filter)

    pg_to_sqlite = {
        "bigint": "INTEGER",
        "smallint": "INTEGER",
        "USER-DEFINED": "TEXT",
        "timestamp with time zone": "DATETIME",
        "timestamp without time zone": "DATETIME",
        "character varying": "TEXT",
        "bytea": "TEXT",
        "text": "TEXT",
        "date": "TEXT",
        "integer": "INTEGER",
        "name": "TEXT",
        "boolean": "NUMERIC",
        "numeric": "decimal",
        "ARRAY": "list"
        }


    for row in cur.fetchall():

        schema_name = row[0]
        table_name = row[1]

        if include_schema_in_table_name:
            tn = __str_to_unicode(schema_name+"."+table_name)
        else:
            tn = __str_to_unicode(table_name)

        column_name = row[2]

        try:
            data_type =  pg_to_sqlite[row[3]]
        except Exception as e:
            print row
            print repr(e)
            sys.exit()

        if not tn in schema["tables"]:
            schema["tables"][tn] = {
                "columns":[],
                "schema_name": schema_name,
                "table_name": table_name,
                "table_name_select_escaped": '"'+schema_name+'"."'+table_name+'"'
                }

        tb = schema["tables"][tn]
        tb["columns"].append({
                "name": column_name,
                "type": data_type
                })

    for (tn, tb) in schema["tables"].items():

        select = """
    SELECT
      tc.table_name, c.column_name, c.data_type
    FROM
      information_schema.table_constraints tc
    JOIN
      information_schema.constraint_column_usage AS ccu
      USING (constraint_schema, constraint_name)
    JOIN
      information_schema.columns AS c
      ON
        c.table_schema = tc.constraint_schema AND
        tc.table_name = c.table_name AND
        ccu.column_name = c.column_name
    WHERE
       constraint_type = 'PRIMARY KEY' AND
       tc.table_name = '%s' AND
       tc.table_schema = '%s' """ % (tb["table_name"], tb["schema_name"])

        __execute(cur, select)

        tb["primary_keys"] = []
        for pk_row in cur.fetchall():
            tb["primary_keys"].append(pk_row[1])

    __filter_schema(
        schema=schema,
        include_tables=include_tables,
        include_tables_exclusive=include_tables_exclusive,
        exclude_tables=exclude_tables
        )

    return schema

def __filter_schema(schema,
                    include_tables=None,
                    include_tables_exclusive=True,
                    exclude_tables=None
                    ):
    # run thru schema an drop not included tables

    tables = schema["tables"]
    err = False
    if include_tables != None:
        # check if all tables are available
        for table_name in include_tables.keys():
            if table_name not in tables:
                print """Error: include_table "%s" not found in origin.""" % (table_name)
                err = True

    if exclude_tables != None:
        # check if all tables are available
        for table_name in exclude_tables:
            if table_name not in tables:
                print """Error: exclude_table "%s" not found in origin.""" % (table_name)
                err = True

    if err:
        print "Available tables:"
        for (table_name, tb) in sorted(tables.items()):
            print table_name
        sys.exit(1)

    for (table_name, tb) in tables.items():
        if exclude_tables != None and table_name in exclude_tables:
            print "Notice: Excluding table \"%s\"." % table_name
            del tables[table_name]
            continue

        if include_tables == None:
            continue

        if table_name not in include_tables:
            if include_tables_exclusive:
                print "Notice: Excluding table \"%s\"." % table_name
                del tables[table_name]
            else:
                # no special rules apply for this table
                print "Notice: Including table \"%s\"." % table_name
            continue

        print "Notice: Including table \"%s\"." % table_name

        incl = include_tables[table_name]
        if incl == True:
            continue

        if "where" in incl:
            tb["where"] = incl["where"]

        if "limit" in incl:
            tb["limit"] = incl["limit"]

        if "orderby" in incl:
            tb["orderby"] = incl["orderby"]

        if "onCreate" in incl:
            tb["onCreate"] = incl["onCreate"]

        if "include_columns" in incl:
            err = False
            for column_name in incl["include_columns"].keys():
                found = False
                for idx, column in enumerate(tb["columns"]):
                    if column["name"] == column_name:
                        found = True
                if not found:
                    print """Error: include_column "%s"."%s" not found in table.""" % (table_name, column_name)
                    err = True

            if err:
                sys.exit(1)

            keep_columns = []
            for idx, column in enumerate(tb["columns"]):
                if column["name"] in incl["include_columns"]:
                    keep_columns.append(column)
                    continue

            tb["columns"] = keep_columns

    return schema

def __create_schema_in_source (name, schema):
    global source_conn

    print """\nCreating tables for "%s" """ % schema["database"]

    tables = schema["tables"]

    for (tn, tb) in tables.items():
        __create_table_in_source(
            origin_database_name = schema["database"],
            origin_type = schema["type"],
            source_name = name,
            origin_table_name = tn,
            table_def = tb
            )
    return

def __create_table_in_source (
    origin_database_name,
    origin_type,
    source_name,
    origin_table_name,
    table_def,
    source_table_name=None
    ):

    global source_conn

    sql = source_conn.cursor()
    sql2 = source_conn.cursor()

    drop = False

    if source_table_name == None:
        __execute(sql2, """
      SELECT origin_id, origin_table_name FROM origin WHERE
         source_name = ? AND
         lower(source_table_name) = lower(?)
     """, (source_name, origin_table_name))

        rows = sql2.fetchall()

        if len(rows):
            source_table_name = origin_table_name+"_"+str(len(rows))
            print "Warning:", origin_table_name, "renamed to:", source_table_name
        else:
            source_table_name = origin_table_name

        source_table_name = source_table_name.lower()
        source_name = source_name.lower()

    else:
        __execute(sql2, """
SELECT origin_id, source_table_name FROM origin WHERE
       source_name = ? AND
       source_table_name = ?""",
                  (source_name, source_table_name))
        rows = sql2.fetchall()
        if len(rows):
            drop = True


    table_def["table_name_in_source"] = source_name+"."+source_table_name
    if drop:
        cmd = """DROP TABLE IF EXISTS "%s" """ % (table_def["table_name_in_source"])
        print "Notice: Dropping existing table: ", table_def["table_name_in_source"]
        __execute(sql, cmd)
    else:
        __execute(sql2, """
     INSERT INTO origin
          (origin_database_name, origin_type, origin_table_name, source_name, source_table_name)
          VALUES (?,?,?,?,?)
     """, (origin_database_name, origin_type, origin_table_name, source_name, source_table_name))

    cmd = """CREATE TABLE "%s" (\n""" % (table_def["table_name_in_source"])
    cmd += "   __source_unique_id TEXT UNIQUE,\n"
    cmd += "   __source_inserted_time DATETIME DEFAULT CURRENT_TIMESTAMP,\n"

    add_comma = False

    for idx, column in enumerate(table_def["columns"]):
        if column["name"] == "__source_unique_id" or column["name"] == "__source_inserted_time":
            # this is a merge from another source
            continue

        if add_comma:
            cmd += ",\n"

        cmd += """  "{0}" {1}""".format(column["name"], column["type"])
        add_comma = True

    pks = table_def["primary_keys"]
    if len(pks) > 0:
        cmd += ",\nPRIMARY KEY ("+'"'+'","'.join(pks)+'")'

    cmd += "\n)"

    # print cmd
    __execute(sql, cmd)

    if "onCreate" in table_def:
        cmd = table_def["onCreate"].replace("%TABLE_NAME_IN_SOURCE%", table_def["table_name_in_source"])

        print """Notice: %s[onCreate]: %s""" % (table_def["table_name_in_source"], cmd)
        __execute(sql, cmd)

def __copy_data_to_source (
    conn,
    schema,
    limit = None
    ):

    global source_conn

    if "database" in schema:
        print """\nCopying data for "%s" """ % schema["database"]

    chunk = 100000

    sql = source_conn.cursor()
    cur = conn.cursor()
    tables = schema["tables"]


    for (table_name, tb) in tables.items():
        qms = []
        column_names = []

        where = ""
        use_limit = limit

        if "where" in tb:
            w = tb["where"]
            if w != None and len(w) > 0:
                where = "WHERE "+w

        if "limit" in tb:
            use_limit = tb["limit"]

        if "orderby" in tb:
            use_orderby = "ORDER BY "+tb["orderby"]
        elif len(tb["primary_keys"]) > 0:
            use_orderby = "ORDER BY \""+"\",\"".join(tb["primary_keys"])+"\""
        else:
            use_orderby = ""

        if use_limit == 0:
            print """Notice: Not copying data for table "%s", request limit is 0. Use None for no limit."""
            continue

        copy_source_unique_id = False
        for idx, column in enumerate(tb["columns"]):
            column_names.append('"'+column["name"]+'"')
            if column["name"] == "__source_unique_id":
                copy_source_unique_id = True

            qms.append("?")

        columns = ",".join(column_names)
        if copy_source_unique_id:
            insert = """INSERT INTO "%s" (%s) VALUES (%s)""" % (tb["table_name_in_source"], columns, ",".join(qms))
        else:
            insert = """INSERT INTO "%s" (__source_unique_id, %s) VALUES (?, %s)""" % (tb["table_name_in_source"], columns, ",".join(qms))

        prefix = "Notice: "+tb["table_name_in_source"]
        print prefix,

        count = 0
        offset = 0
        while True:
            chunk_count = 0

            # limit chunk to the maximium wanted table "limit"
            if use_limit > 0 and offset + chunk > use_limit:
                use_chunk = use_limit - offset
                if use_chunk == 0:
                    break
            else:
                use_chunk = chunk

            if use_chunk > 0:
                lm = "LIMIT "+str(use_chunk)+" OFFSET "+str(offset)
            else:
                lm = ""

            select = """SELECT %s FROM %s %s %s %s""" % (columns, tb["table_name_select_escaped"], where, use_orderby, lm)
            __execute(cur, select)

            for row in cur:
                rec = {}
                for idx, column in enumerate(tb["columns"]):
                    cn = column["name"]
                    if isinstance(row[idx], str):
                        rec[cn] = __str_to_unicode(row[idx])
                    else:
                        rec[cn] = row[idx]

                save_row = []
                if not copy_source_unique_id:
                    pk_data = []
                    for idx, pk_name in enumerate(tb["primary_keys"]):
                        pk_data.append(__value_to_unicode(rec[pk_name]))

                    source_unique_id = "-".join(pk_data)
                    if len(source_unique_id) == 0:
                        source_unique_id = None
                    save_row.append(source_unique_id)

                for idx, column in enumerate(tb["columns"]):
                    save_row.append(rec[column["name"]])

                count += 1
                chunk_count += 1
                # print save_row

                __execute(sql, insert, save_row)

                if count%100==0:
                    print "\r"+prefix, count, "rows...",
                    sys.stdout.flush()

            if chunk == 0 or chunk_count < chunk:
                break
            offset += chunk

        print "\r"+prefix, count, "rows."
    return

def __value_to_unicode (v):
    if isinstance(v, unicode):
        return v
    if isinstance(v, str):
        return __str_to_unicode(v)
    return unicode(v)

def __sqlite_get_schema(conn,
                        include_tables=None,
                        include_tables_exclusive=True,
                        exclude_tables=None
                        ):
    sql = conn.cursor()
    sql.execute("PRAGMA database_list")
    db_name = sql.fetchall()[0][2]

    sql.execute("SELECT type,name,sql FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' AND name NOT LIKE 'seq_%'")
    schema = {
        "tables": {},
        "type": "sqlite",
        "database": db_name

        }

    for row in sql.fetchall():
        tn = __str_to_unicode(row[1])

        schema["tables"][tn] = {
            "table_name": row[1],
            "table_name_select_escaped": '"'+row[1]+'"',
            "columns": []
            }

    for (table_name, tb) in schema["tables"].items():
        sql.execute("""PRAGMA table_info("%s")""" % table_name)
        _pks = {}
        # cid|name|type|notnull|dflt_value|pk
        for row in sql.fetchall():
            tb["columns"].append({
                    "name": row[1],
                    "type": row[2]
                    })
            if row[5] > 0:
                _pks[row[5]-1] = row[1]
        pks = []
        for i in range(5):
            if i in _pks:
                pks.append(_pks[i])
        tb["primary_keys"] = pks


    __filter_schema(
        schema=schema,
        include_tables=include_tables,
        include_tables_exclusive=include_tables_exclusive,
        exclude_tables=exclude_tables
        )

    return schema

def __store_eas_id (
    name, # name of source
    url,  # url of eas
    instance, # instance of eas
    eas_id,  # id for asset
    table_name, # table name in source
    source_unique_id,  # id in source
    column_name,  # column in source
    eas_versions = { "original": ["url"] }
    ):

    _eas_id = str(eas_id)

    req = url+"/bulkversions?instance="+instance+"&asset_ids=["+_eas_id+"]"

    _res = requests.get(req)
    if _res.status_code != 200:
        print """Warning: EAS-ID %s not found or error from EAS-Server. Status: "%s".""" % (eas_id, _res.status_code), _res.text
        return False

    if isinstance(_res.json, dict):
        # old python.requests module: json is an object
        res = _res.json
    else:
        # new python.requests module: json is a method
        res = _res.json()

    count = 0
    skips = 0

    for (eas_version, store_as) in eas_versions.items():
        count += 1
        original = None

        for version in res[_eas_id]["versions"]:
            if version["version"] == "original":
                original = version
                break

        if not original:
            print "Warning: EAS-ID", eas_id, "Original not found."""
            return False

        use_version = None
        for version in res[_eas_id]["versions"]:
            if version["version"] == eas_version:
                use_version = version
                break

        if not use_version:
            print "Notice: EAS-ID", eas_id, "Version", eas_version, """not found."""
            # skip
            skips += 1
            continue

        if use_version["status"] != "done":
            print """Warning: EAS-ID %s, Version "%s" has status "%s", Request: "%s", skipping.""" % (eas_id, use_version["version"], use_version["status"], req)
            # skip
            skips += 1
            continue

        eas_root_id = res[_eas_id]["root_id"]

        if eas_root_id:
            # we need to insert this first, because of a foreign key we
            # have
            ret = __store_eas_id(
                name = name,
                instance = instance,
                url = url,
                eas_id = eas_root_id,
                eas_versions = { eas_version: store_as },
                table_name = table_name,
                source_unique_id = source_unique_id,
                column_name = column_name
                )
            if ret == False:
                print "Warning: Could not insert root id, not storing root_id", eas_root_id
                eas_root_id = None

        __store_file_from_url(
            url = url+use_version["link"],
            name = name,
            source_table_name = table_name,
            source_unique_id = source_unique_id,
            source_column_name = column_name,
            original_filename = original["custom"]["original_filename"],
            file_unique_id = use_version["hash"],
            eas_id = eas_id,
            eas_root_id = eas_root_id,
            file_version = eas_version,
            store_as = store_as
            )

    if skips > 0:
        print "Warning: %s out of %s versions failed to export." % (skips, count)
        return False
    else:
        return True

# stores file as blob from url
def __store_file_from_url (
    name,              # name of source
    url,               # url to fetch data from
    source_table_name, # table this is meant for
    source_unique_id,  # row in table this is meant for
    source_column_name,# column in table this is meant for
    file_unique_id,    # a unique id provided to store the file
    eas_id = None,     # optional eas-id
    eas_root_id = None,# optional eas-root-id
    file_version = None,# optional version
    original_filename = None, # original_filename of file
    store_as = ["data"] # allowed values are "data", "url", "file"
    ):

    if eas_id:
        info = "EAS-ID:"+str(eas_id)+" ["+file_version+"]"
    else:
        info = "URL:"+os.path.basename(url)

    sql = source_conn.cursor()

    # let's see if we already know the file
    __execute(sql, """SELECT filestore_id FROM filestore WHERE unique_id=?""", [file_unique_id])

    for s_as in store_as:
        if s_as in ["data", "url", "file"]:
            if s_as == "file":
                print """Warning: store_as parameter "file" is currently unsupported. """
            continue
        print """Error: store_as parameter needs to be an Array of "data", "url", and/or "file"."""
        sys.exit(1)

    row = sql.fetchone()
    if row:
        filestore_id = row[0]
        print "Notice: Re-used File: ", filestore_id, info # ,r.headers
    else:
        mimetype = None
        blobdata = None
        filesize = None
        extra_info = []

        if "data" in store_as:
            if url.startswith("file://"):
                fn = url[7:] # cut of file
                if not fn.startswith("/"): # relative path, add cwd
                    fn = os.getcwd()+"/"+fn
                    # print "file name", fn
                    # print "file_unique_id", file_unique_id

                fl = open(fn, 'rb')

                with fl:
                    data = fl.read()

                mimetype = None
            else:
                req = requests.get(url)
                mimetype = req.headers["content-type"]
                data = req.content

            filesize = len(data)
            blobdata = sqlite3.Binary(data)

        if url.startswith("file://"):
            filename = url[7:]
        else:
            filename = None

        __execute(sql, """INSERT INTO filestore (
               unique_id,
               original_filename,
               filename,
               url,
               mimetype,
               filesize,
               data
         ) VALUES (?,?,?,?,?,?,?)""", (
                file_unique_id,
                original_filename,
                filename,
                url,
                mimetype,
                filesize,
                blobdata
                ))

        filestore_id = sql.lastrowid

        if original_filename != None:
            extra_info.append(original_filename)

        if mimetype != None:
            extra_info.append(str(mimetype))

        if filesize != None:
            extra_info.append(str(filesize)+"b")

        if len(extra_info):
            extra_info_txt = "("+",".join(extra_info)+")"
        else:
            extra_info_txt = ""


        print "Notice: File:", filestore_id, extra_info_txt.encode("utf8"), "stored in filestore.", info

    __execute(sql, """INSERT INTO file
            (filestore_id,
             source_name,
             source_table_name,
             source_unique_id,
             source_column_name,
             origin_url,
             eas_id,
             eas_root_id,
             file_version
             )

            VALUES (?,?,?,?,?,?,?,?,?)""",
              (filestore_id,
               name,
               source_table_name,
               source_unique_id,
               source_column_name,
               url,
               eas_id,
               eas_root_id,
               file_version
               )
              )

    sql.close()

    return

def __mysql_get_schema(conn,
                       include_tables=None,
                       include_tables_exclusive=True,
                       exclude_tables=None):
    cur = conn.cursor()

    cur.execute("SELECT DATABASE()")
    db_name = cur.fetchall()[0][0]

    cur.execute("""
    SELECT table_name, column_name, data_type
         FROM INFORMATION_SCHEMA.COLUMNS
         WHERE table_schema=DATABASE()
    ORDER BY table_name, ordinal_position""")

    schema = {
        "tables": {},
        "type": "mysql",
        "database": db_name
        }

    mysql_to_sqlite = {
        "varchar": "TEXT",
        "bigint": "INTEGER",
        "text": "TEXT",
        "tinytext": "TEXT",
        "enum": "TEXT",
        "mediumtext": "TEXT",
        "longtext": "TEXT",
        "int": "INTEGER",
        "decimal": "decimal",
        "double": "decimal",
        "tinyint": "INTEGER",
        "smallint": "INTEGER",
        "mediumint": "INTEGER",
        "char": "TEXT",
        "timestamp": "DATETIME",
        "date": "TEXT",
        "datetime": "TEXT"
        }


    for row in cur.fetchall():
        table_name = row[0]

        tn = __str_to_unicode(row[0])

        column_name = row[1]
        data_type = mysql_to_sqlite[row[2]]

        if not tn in schema["tables"]:
            schema["tables"][tn] = {
                "columns":[],
                "table_name": table_name,
                "table_name_select_escaped": '"'+table_name+'"'
                }
        tb = schema["tables"][tn]
        tb["columns"].append({
                "name": column_name,
                "type": data_type
                })

    for (tn, tb) in schema["tables"].items():
        cur.execute("""
        SELECT column_name
             FROM information_schema.key_column_usage k
             WHERE table_schema=DATABASE()
                AND constraint_name='PRIMARY'
                AND table_name='%s'
        ORDER BY table_name, ordinal_position""" % (tb["table_name"]))

        tb["primary_keys"] = []
        for pk_row in cur.fetchall():
            tb["primary_keys"].append(pk_row[0])


    __filter_schema(
        schema=schema,
        include_tables=include_tables,
        include_tables_exclusive=include_tables_exclusive,
        exclude_tables=exclude_tables
        )

    return schema

# http://stackoverflow.com/questions/1094841/reusable-library-to-get-human-readable-version-of-file-size
def __human_size(size_bytes):
    """
    format a size in bytes into a 'human' file size, e.g. bytes, KB, MB, GB, TB, PB
    Note that bytes/KB will be reported in whole numbers but MB and above will have greater precision
    e.g. 1 byte, 43 bytes, 443 KB, 4.3 MB, 4.43 GB, etc
    """
    if size_bytes == 1:
        return "1 byte"

    suffixes_table = [('bytes',0),('KB',0),('MB',1),('GB',2),('TB',2), ('PB',2)]

    num = float(size_bytes)
    for suffix, precision in suffixes_table:
        if num < 1024.0:
            break
        num /= 1024.0

    if precision == 0:
        formatted_size = "%d" % num
    else:
        formatted_size = str(round(num, ndigits=precision))

    return "%s %s" % (formatted_size, suffix)

# source is the output file of this script, the name source stems from the following step in the migration
def prepare_source (
    source="result.sqlite", # filename for the sqlite database file, where the results will be written into
    init=True, # set True and source will be purged
    init_filestore=None # create crucial tables inside the target sqlite file
    ):

    global source_conn

    if init_filestore == None:
        if init == False:
            init_filestore = False
        else:
            init_filestore = True

    try:
        if source_conn:
            __commit_source()
    except NameError:
        pass


    filename = __str_to_unicode(source)

    print >> sys.stderr, """Preparing source "%s" """ % filename

    if init and init_filestore:
        try:
            os.remove(filename)
            print "Old database removed:", filename
        except:
            pass

    # FIXME: if Init==False: check for file existence!

    try:
        source_conn = sqlite3.connect(filename, detect_types=sqlite3.PARSE_DECLTYPES)
    except sqlite3.OperationalError as e:
        print "Error: Unable to open sqlite file: "+filename
        raise e

    source_conn.execute("""PRAGMA foreign_keys=ON""")

    version = source_conn.execute("""SELECT sqlite_version()""").fetchone()[0]
    print """Sqlite %s, Version: %s connected.""" % (filename, version)

    if init:
        if not init_filestore:
            sql = source_conn.cursor()
            row = sql.execute("""SELECT count(filestore_id),sum(filesize) FROM filestore""").fetchone()

            print """Keeping filestore in source "%s", removing all origins. \nFilestore currently holds %s files in %s bytes.""" % (source, row[0], __human_size(row[1]))
            # remove manually
            sql.execute("""SELECT DISTINCT(source_name) FROM origin""")
            for row in sql.fetchall():
                remove_from_source(row[0])
            sql.close()
        else:
            source_conn.execute("""CREATE TABLE origin (
               origin_id INTEGER PRIMARY KEY,
               origin_type TEXT NOT NULL,
               origin_database_name TEXT NOT NULL,
               origin_table_name TEXT NOT NULL,
               source_name TEXT NOT NULL,
               source_table_name TEXT NOT NULL,
               UNIQUE(origin_type, origin_database_name, origin_table_name),
               UNIQUE(source_name, source_table_name)
            )""")

            source_conn.execute("""CREATE TABLE IF NOT EXISTS xmldata (
               node_id_path TEXT NOT NULL,
               node_element_path TEXT NOT NULL,
               node_id INTEGER PRIMARY KEY,
               node_parent_id INTEGER,
               node_level INTEGER NOT NULL,
               node_element TEXT,
               node_attrs TEXT,
               node_data TEXT
           )""")

            __execute(source_conn, """CREATE INDEX xmldata_idx ON xmldata (
               node_id_path
        )""")

            source_conn.execute("""CREATE TABLE IF NOT EXISTS "xmldata.transcribed" (
               node_id_path TEXT NOT NULL,
               node_element_path TEXT NOT NULL
            )""")


        print "Notice: Create Base Tables Sqlite Database \"%s\"" % filename
    else:
        print "Using existing Sqlite Database: \"%s\"" % filename

    if init_filestore:
        if not init:
            print "Notice: Dropping Filestore Tables", filename
            source_conn.execute("""PRAGMA foreign_keys=OFF""")
            __execute(source_conn, """DROP TABLE IF EXISTS \"file\" """)
            __execute(source_conn, """DROP TABLE IF EXISTS \"filestore\" """)
            source_conn.execute("""PRAGMA foreign_keys=ON""")

        __execute(source_conn, """CREATE TABLE file (
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
            )""")


        __execute(source_conn, """CREATE INDEX file_idx ON file (
               source_name,
               source_table_name,
               source_column_name,
               source_unique_id
        )""")

        __execute(source_conn, """CREATE TABLE filestore (
               filestore_id INTEGER PRIMARY KEY,
               unique_id TEXT NOT NULL UNIQUE,
               original_filename TEXT,
               mimetype TEXT,
               filesize INTEGER,
               url TEXT,
               filename TEXT,
               data BLOB
            )""")

        print "Notice: Created Filestore Tables:", filename

# remove all data from the source for a
# specific name
def remove_from_source (
    name, # name of source
    keep_filestore=False # whether to keep or drop files from filestore
    ):
    global source_conn

    print "\nRemoving all tables for source", name, ".",
    if keep_filestore:
        print "Keeping files in filestore..."
    else:
        print "Removing matching files from filestore..."

    sql = source_conn.cursor()
    __execute(sql, "SELECT source_name, source_table_name FROM origin WHERE source_name=?", [name])
    for row in sql.fetchall():
        tntpl = (row[0], row[1])
        __execute(sql, """DROP TABLE IF EXISTS "%s.%s" """ % tntpl)

    if not keep_filestore:
        __execute(sql, """
             DELETE FROM filestore
             WHERE filestore_id IN (
                SELECT filestore_id FROM file WHERE source_name=?
             )""", [name])
        __execute(sql, """DELETE FROM file WHERE source_name=?""", [name])
    __execute(sql, """DELETE FROM origin WHERE source_name=?""", [name])

def pg_to_source(
    name,
    dsn,
    schema_name=None,
    limit=None,
    include_tables=None,
    include_tables_exclusive=True,
    include_schema_in_table_name=True,
    exclude_tables=None
    ):

    conn = psycopg2.connect(dsn)
    schema = __pg_get_schema(conn=conn, schema_name=schema_name, include_tables=include_tables, include_schema_in_table_name=include_schema_in_table_name, include_tables_exclusive=include_tables_exclusive, exclude_tables=exclude_tables)
    __create_schema_in_source(name=name, schema=schema)
    __copy_data_to_source(conn=conn, schema=schema, limit=limit)
    conn.close()

def sqlite_to_source(
    name,
    filename,
    limit=None,
    include_tables=None,
    include_tables_exclusive=True,
    exclude_tables=None
    ):

    conn = sqlite3.connect(filename, detect_types=sqlite3.PARSE_DECLTYPES)
    schema = __sqlite_get_schema(conn=conn, include_tables=include_tables, include_tables_exclusive=include_tables_exclusive, exclude_tables=exclude_tables)
    __create_schema_in_source(name=name, schema=schema)
    # we use a byte string here, so we can detect
    # the correct charset
    conn.text_factory = str
    __copy_data_to_source(conn=conn, schema=schema, limit=limit)
    conn.close()

def eas_to_source (name, url, instance, eas_versions):
    global source_conn

    print "Notice: Loading data from EAS:", url, "Instance: ", instance, "Versions:", repr(eas_versions)

    sql = source_conn.cursor()
    sql2 = source_conn.cursor()
    sql.execute("""SELECT table_name, name FROM "%s.eadb_columns" WHERE type='easfile'"""%(name))
    eas_cols = 0
    for row in sql.fetchall():
        table_name = row[0]
        column_name = row[1]
        cmd = """SELECT origin_id, source_table_name FROM origin WHERE source_name="%s" AND (source_table_name LIKE "%%.%s" OR source_table_name = "%s") """ % (name, table_name, table_name)
        # print cmd
        __execute(sql2, cmd)
        _row = sql2.fetchone()
        if _row == None:
            print "Notice: Table '%s.%s[%s]' not found in source, unable to import EAS files." % (name, table_name, column_name)
            continue

        eas_cols += 1
        source_table_name = _row[1]

        cmd = """SELECT "%s", __source_unique_id FROM "%s.%s" WHERE "%s" > 0 AND "%s" != '%s' """ % (column_name, name, source_table_name, column_name, column_name, column_name)
        print "Notice: Importing %s.%s..." %(table_name, column_name)
        __execute(sql2, cmd)
        count = 0
        for row2 in sql2.fetchall():
            count += 1
            __store_eas_id(
                name = name,
                instance = instance,
                url = url,
                eas_id = row2[0],
                table_name = source_table_name, #table_name,
                source_unique_id = row2[1],
                column_name = column_name,
                eas_versions = eas_versions
                )

        print "Notice: Done Importing %s.%s. Imported %s files." %(table_name, column_name, count)

    if eas_cols == 0:
        print "Warning: No EAS columns found and nothing imported for '%s'." % (name)

    sql2.close()
    sql.close()

def mysql_to_source(
    name,
    host,
    db,
    user="",
    passwd="",
    limit=None,
    include_tables=None,
    include_tables_exclusive=True,
    exclude_tables=None
    ):

    conn = MySQLdb.connect(host=host, db=db, user=user, passwd=passwd)

    conn.cursor().execute("SET SQL_MODE=ANSI_QUOTES;")

    schema = __mysql_get_schema(conn=conn, include_tables=include_tables, include_tables_exclusive=include_tables_exclusive, exclude_tables=exclude_tables)
    __create_schema_in_source(name=name, schema=schema)
    __copy_data_to_source(conn=conn, schema=schema, limit=limit)
    conn.close()

def get_source_conn():
    global source_conn
    return source_conn

def csv_to_source (
    name,             # name in source
    filename,         # csv filename
    columns=None,     # columns array with "name" and "type" as keys, defaults to first row in csv
    table_name=None,  # table name in source, defaults to filename's basename
    dialect="detect"  # csv dialect
    ):
    global source_conn
    sql = source_conn.cursor()

    print "Notice: Reading CSV file", "\""+filename+"\"", "Dialect:", dialect

    if dialect == "detect":
        with open(filename, 'rb') as csvfile:
            _dialect = csv.Sniffer().sniff(csvfile.read(2024))
        print "Notice: Detected dialect."
    else:
        _dialect = csv.get_dialect(dialect)

    quoting_modes = dict( (getattr(csv,n), n) for n in dir(csv) if n.startswith('QUOTE_') )

    # print "  delimiter   = %-6r    skipinitialspace = %r" % (_dialect.delimiter, _dialect.skipinitialspace)
    # print "  doublequote = %-6r    quoting          = %s" % (_dialect.doublequote, quoting_modes[_dialect.quoting])
    # print "  quotechar   = %-6r    lineterminator   = %r" % (_dialect.quotechar, _dialect.lineterminator)
    # print "  escapechar  = %-6r" % _dialect.escapechar

    csvfile = open(filename, 'rb')
    reader = csv.reader(csvfile, _dialect)
    row_count = 0
    if columns == None:
        header = reader.next()
        columns = []
        idx = 0
        for column_name in header:
            idx = idx + 1
            cn = column_name.strip()
            if len(column_name) == 0:
                cn = "column_"+str(idx)

            columns.append({"name": cn, "type": "TEXT"})
        row_count += 1

    if table_name == None:
        table_name = os.path.basename(filename)

    qms = ["?"]
    column_names = []
    for column in columns:
        qms.append("?")
        column_names.append('"'+column["name"]+'"')

    table_def = {
        "columns": columns,
        "primary_keys": []
        }

    __create_table_in_source(
        origin_database_name = os.path.abspath(filename),
        origin_type = "csv",
        source_name = name,
        table_def = table_def,
        origin_table_name = table_name
        )

    insert = """INSERT INTO "%s" (__source_unique_id, %s) VALUES(%s)""" % (table_def["table_name_in_source"],",".join(column_names), ",".join(qms))

    for row in reader:
        bindings = []
        bindings.append(row_count)
        for s in row:
            if len(bindings) == len(qms):
                print "Warning: Too many columns found in row %s, ignoring additional columns." % row_count
                break
            bindings.append(__value_to_unicode(s))

        if len(bindings) < len(qms):
            # fill with space
            for i in range(0, len(qms)-len(bindings)):
                bindings.append(u"")

        try:
            __execute(sql, insert, bindings)
        except Exception as e:
            print "Row["+str(row_count)+"]:", row
            raise e

        row_count += 1

    csvfile.close()

def __chunks (l, n):
    n = max(1, n)
    return [l[i:i + n] for i in range(0, len(l), n)]

def xml_save_node(
    # node_type,
    node_element=None,
    node_attrs=None,
    node_attr_key=None,
    node_attr_value=None,
    node_data=None
    ):

    global xml_cursor, xml_node_id_stack, xml_node_element_stack

    if len(xml_node_id_stack) == 0:
        node_parent_id = None
    else:
        node_parent_id = int(xml_node_id_stack[-1])

    node_id_path = ".".join(xml_node_id_stack)
    node_element_path = "|".join(xml_node_element_stack)
    node_level = len(xml_node_id_stack)

    if node_attrs != None and len(node_attrs) == 0:
        node_attrs = None

    __execute(xml_cursor, """INSERT INTO xmldata (node_parent_id, node_id_path, node_element_path, node_level, node_element, node_attrs, node_data) VALUES (?,?,?,?,?,?,?)""", (node_parent_id, node_id_path, node_element_path, node_level, node_element, node_attrs, node_data)) #node_attr_key, node_attr_value, node_type,

    # print "saving node", xml_cursor.lastrowid, node_parent_id #, node_type
    return xml_cursor.lastrowid


def start_element(name, attrs):
    global xml_in_cdata, xml_text, xml_depth, xml_node_id_stack, xml_count, xml_node_element_stack

    # xml_count = xml_count + 1
    # print "\r Elements: ", xml_count,

    node_id = xml_save_node(node_element = name, node_attrs = attrs)
    xml_node_id_stack.append(str(node_id))
    xml_node_element_stack.append(name)

    # for attr_key, attr_value in attrs.iteritems():
    #     save_xml_node(node_type = "ATTR", node_attr_key = attr_key, node_attr_value = attr_value)

    # print "".join(xml_depth), name, attrs
    xml_depth.append("  ")

def end_element(name):
    global xml_in_cdata, xml_text, xml_depth, xml_node_id_stack, xml_cursor, xml_node_element_stack, xml_transcribed_counter, xml_filename

    node_id = int(xml_node_id_stack[-1])

    if len(xml_text) > 0:
        node_data = "".join(xml_text).strip()

        if len(node_data) > 0:
            # update data in element node
            __execute(xml_cursor, """UPDATE xmldata SET "node_data" = ? WHERE node_id = ?""", [node_data, node_id])

        # save_xml_node(node_type = "DATA", node_data = node_data)
        # print "".join(xml_depth), node_data
        del xml_text[:]

    __execute(xml_cursor, "SELECT node_id_path, node_id, node_element_path, node_element, node_attrs FROM xmldata WHERE node_id = %s" % node_id)
    row = xml_cursor.fetchone()

    node_id_path = row[0]+str(row[1])
    node_element_path = row[2]+row[3]
    if row[4] != None:
        node_attrs = json.loads(row[4])
    else:
        node_attrs = None
    data_by_element = {}

    if node_attrs != None:
        for key, value in node_attrs.iteritems():
            # node attributes
            data_by_element["attr:"+key] = value.strip()

    __execute(xml_cursor, "SELECT node_element, node_data, node_id_path, node_element_path FROM xmldata WHERE node_id_path = '%s' ORDER BY node_id" % (".".join(xml_node_id_stack)))

    for row in xml_cursor.fetchall():
        node_element = row[0]
        node_data = row[1]
        if node_data == None:
            continue
        node_id_path = row[2]
        node_element_path = row[3]
        if node_element not in data_by_element:
            data_by_element[node_element] = []
        data_by_element[node_element].append(node_data)

    if len(data_by_element):
        # print node_id_path, node_element_path

        columns = []
        questionmarks = []
        data = [node_id_path, node_element_path]

        for node_element, node_data in data_by_element.iteritems():
            columns.append(node_element)
            questionmarks.append("?")
            if len(node_data) == 1:
                data.append(node_data[0])
            else:
                data.append(node_data)

        for node_element in sorted(columns):
            xml_require_column(node_element)


        __execute(xml_cursor, """INSERT INTO "xmldata.transcribed" (node_id_path, node_element_path, "%s") VALUES (?,?,%s)""" % ('","'.join(columns), ",".join(questionmarks)), data)
        # print "INSERT", repr(data)
        xml_transcribed_counter = xml_transcribed_counter + 1
        if xml_transcribed_counter % 10 == 0:
            print "\r ", xml_filename, xml_transcribed_counter,

    xml_node_id_stack.pop()
    xml_node_element_stack.pop()
    xml_depth.pop()
    # print "".join(depth), "/", name

# adds a column to the transcribed table if not exists
def xml_require_column(name):
    global xml_transcribed_columns

    if xml_transcribed_columns == None:
        xml_transcribed_columns = []
        __execute(xml_cursor, """PRAGMA table_info("xmldata.transcribed")""")
        for row in xml_cursor.fetchall():
            xml_transcribed_columns.append(row[1])

    if name in xml_transcribed_columns:
        return

    xml_cursor2 = source_conn.cursor()
    __execute(xml_cursor2, """ALTER TABLE "xmldata.transcribed" ADD COLUMN "%s" TEXT""" % name)
    xml_transcribed_columns.append(name)
    # print """Added "xmldata.transcribed"."%s".""" % name
    xml_cursor2.close()


def start_cdata():
    global xml_in_cdata, xml_text, xml_depth

    xml_in_cdata = True

def end_cdata():
    global xml_in_cdata, xml_text, xml_depth

    xml_in_cdata = False

def char_data(data):
    global xml_in_cdata, xml_text, xml_depth
    xml_text.append(data)
    return

# the code below tries to be smart with white-space. i don't know
# what the right thing to do is, so for now we leave it out
#
# if xml_in_cdata:
#     xml_text.append(data)
# else:
#     if len(data.strip()) == 0:
#         return
#     xml_text.append(data.lstrip())

def xml_to_source(
    basedir,         # basedir which is not saved
    filename,        # xml filename
    name = None      # name in source
    ):
    global xml_in_cdata, xml_cdata, xml_text, xml_depth, xml_node_id_stack, xml_node_element_stack, xml_cursor, xml_count
    global source_conn
    global xml_transcribed_columns, xml_transcribed_counter
    global xml_filename

    xml_transcribed_columns = None
    try:
        xml_transcribed_counter
    except NameError:
        xml_transcribed_counter = 0
    xml_cursor = source_conn.cursor()
    xml_count = 0

        # node_type TEXT NOT NULL,
        # node_attr_key TEXT,
        # node_attr_value TEXT,

    # print "Importing XML", repr(filename)

    xml_depth = []
    xml_node_id_stack = []
    xml_node_element_stack = []
    xml_text = []
    xml_in_cdata = False
    xml_cdata = []

    p = xml.parsers.expat.ParserCreate()

    p.buffer_text = False
    p.ordered_attributes = False

    p.StartElementHandler = start_element
    p.EndElementHandler = end_element
    p.StartCdataSectionHandler = start_cdata
    p.EndCdataSectionHandler = end_cdata
    p.CharacterDataHandler = char_data

    filepath = filename[len(basedir)+1:]
    xml_filename = os.path.basename(filename)

    start_element(filepath, {
            "path": os.path.dirname(filename),
            "basedir": basedir,
            "filepath": filepath,
            "filename": xml_filename
            })

    with open(filename) as inf:
        p.ParseFile(inf)

    # we don't right an end element here, so we don't have this in our
    # transcribed table

    print "\r ", xml_filename, xml_transcribed_counter

def merge_source (filename):
    global source_conn

    print "Notice: Merging source:", filename
    conn = sqlite3.connect(filename, detect_types=sqlite3.PARSE_DECLTYPES)
    schema = __sqlite_get_schema(conn=conn)

    if "origin" not in schema["tables"]:
        print "Error: Sqlite Database does not contain table \"origin\", no valid source detected."
        sys.exit(1)

    sql = conn.cursor()
    sql2 = conn.cursor()
    sql3 = source_conn.cursor()

    select = """SELECT origin_type, origin_database_name, origin_table_name, source_name, source_table_name FROM origin"""

    __execute(sql, select)

    for row in sql.fetchall():
        source_name = row[3]
        source_table_name = row[4]
        # __merge_table
        print "Notice: Merging table", source_table_name, ":", row[0], row[1], row[2]

        table_def = schema["tables"][source_name+"."+source_table_name]

        __create_table_in_source(
            origin_database_name = row[1],
            origin_type = row[0],
            source_name = source_name,
            origin_table_name = row[2],
            table_def = table_def,
            source_table_name = source_table_name
            )

        short_schema = {
            "tables": {}
            }
        short_schema["tables"] = {}
        short_schema["tables"][source_table_name] = table_def

        __copy_data_to_source(conn, short_schema)

        # now we need to copy filestore and file
        select = """SELECT filestore_id, unique_id, original_filename, mimetype, filesize, url, filename, data FROM filestore WHERE filestore_id IN (SELECT filestore_id FROM "file" WHERE source_name='%s' and source_table_name='%s')""" % (source_name, source_table_name)
        __execute(sql2, select)

        insert = "INSERT INTO filestore (unique_id, original_filename, mimetype, filesize, url, filename, data) VALUES (?,?,?,?,?,?,?)"

        filestore_id_map = {}

        for _row in sql2.fetchall():
            filestore_id = _row[0]

            bindings = []
            for idx in range(1,8):
                bindings.append(_row[idx])

            __execute(sql3, insert, bindings)
            filestore_id_map[filestore_id] = sql3.lastrowid

        # delete files from the same source and an earlier merge
        __execute(sql3, """DELETE FROM file WHERE source_name='%s' AND source_table_name='%s' """ %(source_name, source_table_name))

        select = """SELECT filestore_id, source_name, source_table_name, source_column_name, source_unique_id, origin_url, eas_id, eas_root_id, file_version FROM "file" WHERE source_name='%s' and source_table_name='%s'""" % (source_name, source_table_name)

        insert = "INSERT INTO file (filestore_id, source_name, source_table_name, source_column_name, source_unique_id, origin_url, eas_id, eas_root_id, file_version) VALUES (?,?,?,?,?,?,?,?,?)"

        __execute(sql2, select)
        files = 0
        for _row in sql2.fetchall():
            bindings = []
            bindings.append(filestore_id_map[_row[0]])
            for idx in range(1,9):
                bindings.append(_row[idx])
            __execute(sql3, insert, bindings)
            files += 1

        if files > 0:
            print "Notice:", files, "files copied."

def dump_mysql(output, encode_blob_method="hex", blob_chunk_size=50000):
    global source_conn
    schema = __sqlite_get_schema(source_conn)
    sql = source_conn.cursor()

    out = open(output, "wb")

    tables = schema["tables"]
    def write_out(s):
        out.write(s.encode("utf8"))

    write_out("SET sql_mode=\"NO_BACKSLASH_ESCAPES,ANSI_QUOTES\";\n")
    write_out("BEGIN;\n")

    sqlite_to_mysql = {
        "TEXT": "TEXT",
        "integer": "BIGINT",
        "INTEGER": "BIGINT",
        "DATETIME": "TIMESTAMP",
        "BLOB": "LONGBLOB"
        }

    for (table_name, tb) in tables.items():
        qms = []
        column_names = []
        cmd = "CREATE TABLE \""+table_name+"\" (\n"
        for idx, column in enumerate(tb["columns"]):
            if idx > 0:
                cmd += ",\n"

            _type = column["type"]
            if _type in sqlite_to_mysql:
                __type = sqlite_to_mysql[_type]
            else:
                __type = "TEXT"


            cmd += "   \""+column["name"]+"\" "+__type
            column_names.append("\""+column["name"]+"\"")

        cmd += "\n);\n"
        write_out(cmd)

        select = """SELECT %s FROM "%s" """ % (",".join(column_names), table_name)

        __execute(sql, select)
        for row in sql.fetchall():
            values = []
            more_cmds = []
            where_clause = None

            for (idx, item) in enumerate(row):
                if item == None:
                    values.append("null")
                elif isinstance(item, unicode):
                    values.append("'"+(item.replace("'","''"))+"'")
                elif isinstance(item, int):
                    values.append(str(item))
                elif isinstance(item, buffer):

                    # SPLIT BLOB into handlable chunks for mysql
                    # mysql has a server limit of 1MB for query packets
                    if encode_blob_method == "base64": # requires mysql >= 5.6.1
                        s = str(item).encode("base64")
                        decode = "FROM_BASE64"
                    elif encode_blob_method == "hex":
                        s = str(item).encode("hex")
                        decode = "UNHEX"
                    else:
                        raise Exception("Unknown encode_blob_method: "+encode_blob_method)

                    if len(s) < blob_chunk_size:
                        values.append(decode+"('"+s+"')")
                        continue

                    if where_clause == None:
                        ands = []
                        for pk in tb["primary_keys"]:
                            value = None
                            for (cidx, column) in enumerate(tb["columns"]):
                                if column["name"] == pk:
                                    value = row[cidx]
                                    if value == None:
                                        raise Exception("Primary Key Data not found for "+pk)
                                    ands.append("\""+pk+"\"="+__value_to_unicode(value))
                        where_clause = "WHERE "+" AND ".join(ands)

                    cn = tb["columns"][idx]["name"]
                    for (cidx, chunk) in enumerate(__chunks(s, blob_chunk_size)):
                        if cidx == 0:
                            values.append(decode+"('"+chunk+"')")
                        else:
                            more_cmds.append("UPDATE \""+table_name+"\" SET \""+cn+"\" = \""+cn+"\"+"+decode+"('"+chunk+"') "+where_clause+";\n")
                else:
                    print "Warning: Unable to insert item", table_name, type(item)

            cmd = "INSERT INTO \""+table_name+"\" VALUES ("+",".join(values)+");\n"
            write_out(cmd)
            if len(more_cmds) > 0:
                for more_cmd in more_cmds:
                    write_out(more_cmd)


    write_out("COMMIT;\n")
    out.close()
    print "Notice: Dumped current sqlite to mysql file: ", output

