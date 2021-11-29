# coding=utf8

import json
import sqlite3
import hashlib
from datetime import datetime
import urllib.request, urllib.error, urllib.parse


import_type_array_map = {
    "tags": ("tags", "POST"),
    "pool": ("pools", "POST"),
    "user": ("users", "POST"),
    "group": ("groups", "POST"),
    "db": ("objects", "POST"),
    "collection": ("collections", "PUT")
}


def connect_to_sqlite(filename, detect_types=sqlite3.PARSE_DECLTYPES):
    try:
        c = sqlite3.connect(filename, detect_types)
        version = c.execute("""SELECT sqlite_version()""").fetchone()[0]
        print("""Sqlite %s, Version: %s connected.""" % (filename, version))
        return c
    except sqlite3.OperationalError as e:
        print("Error: Unable to open sqlite file: " + filename)
        raise e


def commit(con, close=False):
    try:
        if con:
            con.commit()
            if close:
                con.close()
                print("closed connection")
    except Exception as e:
        print("Error: Unable to commit: ", e)

# helper methods


def generate_hash_reference(value):
    return hashlib.md5(value.encode("utf-8")).hexdigest()


ISO_FORMAT_OUTPUT = "%Y-%m-%dT%H:%M:%S"
DATE_FORMAT_OUTPUT = "%Y-%m-%d"


def datetime_to_iso(d):
    return d.strftime(ISO_FORMAT_OUTPUT)


def datetime_to_date(d):
    return d.strftime(DATE_FORMAT_OUTPUT)


def to_easydb_date_object(d):
    return {
        "value": d
    }


def print_js(js, _ind=4):
    if _ind == 'compact':
        return json.dumps(js, indent=None, separators=(',', ':'))
    return json.dumps(js, indent=_ind)


def format_time_diff(t_start, note):
    t_diff = datetime.now() - t_start
    return note + " took " + str(t_diff.total_seconds()) + " sec"


# recursive generating of payloads of hierarchic objects
def build_hierarchic_objects(output_file, hierarchy, objecttype, pool_reference=None, mapping={}, parent_ref=None, first_object=True, reference_column=None):
    count = 0
    for h in hierarchy:
        # create unique reference by appending a hashsum of the parent
        reference = hierarchy[h][reference_column]

        payload = {
            # default fields
            "_mask": "_all_fields",
            "_objecttype": objecttype,

            # object fields
            objecttype: {
                # default object fields
                "_version": 1,
                "_id": None
            }
        }

        # migration reference
        if reference_column is not None:
            payload[objecttype][reference_column] = o[reference_column]

        if pool_reference is not None:
            payload["_pool"] = {
                "pool": format_lookup("_id", "reference", pool_reference)
            }

        # parent reference for hierarchic object
        if parent_ref is not None:
            payload[objecttype]["lookup:_id_parent"] = parent_ref

        # mapped object fields
        for m in mapping:
            key = mapping[m]
            if not m in hierarchy[h]:
                continue
            if isinstance(key, str):
                payload[objecttype][key] = hierarchy[h][m]
            elif isinstance(key, tuple):
                # NESTED: ("_nested", "fundorte__synonyme", "synonym")
                if len(key) == 3 and key[0] == "_nested":
                    nested = hierarchy[h][m]
                    if isinstance(nested, list):
                        payload[objecttype][key[0] + ":" + key[1]] = [
                            {key[2]: nested_value} for nested_value in nested
                        ]
                # LINK
                elif len(key) == 4 and key[0] == "link":
                    payload[objecttype][key[1]] = {
                        key[2]: {
                            "lookup:_id": {
                                key[3]: o[m]
                            }
                        },
                        "_objecttype": key[2],
                        "_mask": "_all_fields"
                    }

        # save the object
        if first_object:
            first_object = False
        else:
            output_file.write(', ')

        output_file.write(json.dumps(payload, indent=4))

        count += 1

        if "children" in hierarchy[h]:
            count += build_hierarchic_objects(
                output_file=output_file,
                hierarchy=hierarchy[h]["children"],
                objecttype=objecttype,
                # pool_reference = pool_reference,
                mapping=mapping,
                parent_ref={
                    reference_column: reference
                },
                first_object=False,
                reference_column=reference_column)

    return count


def apply_mapping(obj, mapping, payload, objecttype):
    for m in mapping:
        key = mapping[m]
        if not m in obj:
            continue
        if isinstance(key, str):
            payload[objecttype][key] = obj[m]
        elif isinstance(key, tuple):
            # NESTED: ("_nested", "fundorte__synonyme", "synonym")
            if len(key) == 3 and key[0] == "_nested":
                nested = obj[m]
                if isinstance(nested, list):
                    payload[objecttype][key[0] + ":" + key[1]] = [
                        {key[2]: nested_value} for nested_value in nested
                    ]
            # LINK
            elif len(key) == 4 and key[0] == "link":
                payload[objecttype][key[1]] = {
                    key[2]: {
                        "lookup:_id": {
                            key[3]: obj[m]
                        }
                    },
                    "_objecttype": key[2],
                    "_mask": "_all_fields"
                }

    return obj


def build_objects(output_file, objects, objecttype, pool_reference=None, mapping={}, first_object=True, reference_column=None):
    count = 0
    for o in objects:
        payload = {
            # default fields
            "_mask": "_all_fields",
            "_objecttype": objecttype,

            # object fields
            objecttype: {
                # default object fields
                "_version": 1,
                "_id": None
            }
        }

        # migration reference
        if reference_column is not None:
            payload[objecttype][reference_column] = o[reference_column]

        if pool_reference is not None:
            payload[objecttype]["_pool"] = {
                "pool": format_lookup("_id", "reference", pool_reference)
            }

        o = apply_mapping(o, mapping, payload, objecttype)

        # save the object
        if first_object:
            first_object = False
        else:
            output_file.write(', ')

        output_file.write(json.dumps(payload, indent=4))

        count += 1
        # print("wrote",count,"objects")

    return count


def build_object_updates(output_file, objects, objecttype, reference_column, version, pool_reference=None, mapping={}, first_object=True):
    count = 0
    for o in objects:
        payload = {
            # default fields
            "_mask": "_all_fields",
            "_objecttype": objecttype,

            # object fields
            objecttype: {
                # default object fields
                "_version": version,
                "lookup:_id": {
                    reference_column: o[reference_column]
                }
            }
        }

        # migration reference
        payload[objecttype][reference_column] = o[reference_column]

        if pool_reference is not None:
            payload[objecttype]["_pool"] = {
                "pool": format_lookup("_id", "reference", pool_reference)
            }

        o = apply_mapping(o, mapping, payload, objecttype)

        # save the object
        if first_object:
            first_object = False
        else:
            output_file.write(', ')

        output_file.write(json.dumps(payload, indent=4))

        count += 1
        # print("wrote",count,"objects")

    return count


def save_batch(payload, folder, filename, importtype, manifest, objecttype=None):
    data = {
        "import_type": importtype,
        import_type_array_map[importtype][0]: payload
    }
    if objecttype is not None:
        data["objecttype"] = objecttype

    f = open(folder + filename, "w")
    f.write(json.dumps(data, indent=4))
    f.close()

    manifest["payloads"].append(filename)
    if objecttype is None:
        print("saved importtype", importtype, "as", filename)
    else:
        print("saved objecttype", objecttype, "as", filename)
    return manifest


def check_image_url_reachable(url, verbose=False):
    try:
        request = urllib.request.Request(url)
        request.get_method = lambda: 'HEAD'
        response = urllib.request.urlopen(request)
        if response.getcode() == 200:
            if verbose:
                print("URL", url, "reachable")
            return True
        else:
            print("URL", url, "unreachable, Code:", response.getcode())
            if verbose:
                print(response.info())
            return False
    except Exception as e:
        print("URL", url, "unreachable, Error:", e)
        return False


# helper methods for output of headers, footers etc

def write_header(filename, objecttype, import_type="db"):
    output_file = open(filename, 'w')
    output_file.write("""
    {
        "import_type": "%s",
        "objecttype": "%s",
        "objects": [
    """ % (import_type, objecttype))
    output_file.close()
    # print("wrote header")


def write_footer(filename):
    output_file = open(filename, 'a')
    output_file.write("""
        ]
    }
    """)
    output_file.close()
    # print("wrote footer")


# save hierarchic objects

def write_payload_hierarchic_objects(output_filename, hierarchy, objecttype, pool_reference=None, mapping={}, reference_column="reference"):

    print("Output File:", output_filename)

    write_header(output_filename, objecttype)

    # objects
    output_file = open(output_filename, 'a')
    object_count = build_hierarchic_objects(
        output_file=output_file,
        hierarchy=hierarchy,
        pool_reference=pool_reference,
        objecttype=objecttype,
        mapping=mapping,
        reference_column=reference_column
    )
    output_file.close()
    # print("wrote", object_count, "objects")

    write_footer(output_filename)


def write_payload_list_objects(output_filename, objects, objecttype, pool_reference=None, mapping={}, reference_column="reference"):

    print("Output File:", output_filename)

    write_header(output_filename, objecttype)

    # objects
    output_file = open(output_filename, 'a')
    object_count = build_objects(
        output_file=output_file,
        objects=objects,
        objecttype=objecttype,
        pool_reference=pool_reference,
        mapping=mapping,
        reference_column=reference_column
    )
    output_file.close()
    # print("wrote", object_count, "objects")

    write_footer(output_filename)


def format_lookup(key, ref_column, ref_value, only_inner=False):
    l = {
        ref_column: ref_value
    }
    if only_inner:
        return l
    return {
        "lookup:" + key: l
    }


def execute_statement(cursor, sql, connection=None, params=[], commit=False, close_connection=False, verbose=False):
    if verbose:
        print("SQL:", sql.strip())
        if len(params) > 0:
            print("Parameters:", ", ".join([str(p) for p in params]))

    t_sql_start = datetime.now()
    res = cursor.execute(sql, params)
    rows = res.fetchall()

    if commit and connection is not None:
        if verbose:
            print("Closing connection")
        __commit(connection, close_connection)

    return rows


def build_dante_plugin_code(uri, name):
    return {
        "_fulltext": {
            "string": uri if uri is not None else "",
            "text": name
        },
        "conceptURI": uri if uri is not None else "",
        "conceptName": name
    }


# In[260]:


def get_nested_name(name):
    return "_nested:" + name


def insert_into_nested(nested_name, nested_table, nested_list={}):
    nested_name = get_nested_name(nested_name)
    if not nested_name in nested_list:
        nested_list[nested_name] = []
    nested_list[nested_name].append(nested_table)
    return nested_list


def build_nested_entry_for_dante(nested_name, dante_uri, dante_name, plugin_name, freitext=None, nested_list={}):
    nested = {
        plugin_name: build_dante_plugin_code(dante_uri, dante_name)
    }
    if freitext is not None and freitext[1] != "0":
        nested[freitext[0]] = freitext[1]

    return insert_into_nested(nested_name, nested, nested_list)


def insert_field_into_object(objecttype, field_name, field_value, obj):
    if objecttype not in obj:
        # print("objecttype",objecttype,"not found in object!")
        return obj
    if field_name in obj[objecttype]:
        # print("field",objecttype+"."+field_name,"already in object!")
        return obj
    obj[objecttype][field_name] = field_value
    return obj


def breadth_first_batches(objects, objecttype, batches, parent_key="lookup:_id_parent", parents=[], reference_column="reference"):
    parent_lookups = []
    sorted_objects = []
    if len(parents) < 1:
        for o in objects:
            if not parent_key in o[objecttype]:
                parent_lookups.append(o[objecttype][reference_column])
                sorted_objects.append(o)
    else:
        for p in parents:
            for o in objects:
                if not parent_key in o[objecttype]:
                    continue
                if o[objecttype][parent_key][reference_column] == p:
                    parent_lookups.append(o[objecttype][reference_column])
                    sorted_objects.append(o)
    if len(sorted_objects) > 0:
        batches.append(sorted_objects)
        print("collected", len(sorted_objects), "in the current depth")
    if len(parent_lookups) > 0:
        breadth_first_batches(objects, objecttype, batches,
                              parent_key, parent_lookups, reference_column)
    return batches


def convert_hierarchy_to_batches(path, filename, objecttype, manifest, reference_column="reference", batch_size=None):
    if len(path) > 0 and not path[-1] == "/":
        path += "/"
    depth_ordered_batches = breadth_first_batches(
        objects=json.loads(open(path + filename).read())["objects"],
        objecttype=objecttype,
        batches=[],
        reference_column=reference_column
    )
    depth = 0

    if batch_size is None:
        for b in depth_ordered_batches:
            batch_filename = "%s_%03d.json" % (objecttype, depth)

            write_header(path + batch_filename, objecttype)

            output_file = open(path + batch_filename, 'a')
            first_object = True
            for ob in b:
                if first_object:
                    first_object = False
                else:
                    output_file.write(",\n")
                output_file.write(json.dumps(ob, indent=4))
            output_file.close()

            write_footer(path + batch_filename)

            manifest["payloads"].append(batch_filename)
            print("saved batch", depth, "as", batch_filename)

            depth += 1

    else:

        for b in depth_ordered_batches:
            batch = 0

            current_batch_size = 0
            for i in range(len(b)):
                first_object = False
                if current_batch_size == 0:
                    batch_filename = "%s_%03d_batch_%03d.json" % (
                        objecttype, depth, batch)
                    write_header(path + batch_filename, objecttype)
                    first_object = True

                output_file = open(path + batch_filename, 'a')
                ob = b[i]
                if first_object:
                    first_object = False
                else:
                    output_file.write(",\n")
                output_file.write(json.dumps(ob, indent=4))
                output_file.close()
                current_batch_size += 1

                if current_batch_size >= batch_size or i >= len(b) - 1:
                    write_footer(path + batch_filename)
                    first_object = True
                    current_batch_size = 0
                    batch += 1

                    manifest["payloads"].append(batch_filename)
                    print("saved batch", depth, batch, "as", batch_filename)

            depth += 1

    return manifest


def export_basetype(basetype, folder, manifest, objects):
    filename = "basetype_%s.json" % (basetype)
    f = open(folder + "/" + filename, "w")
    f.write(json.dumps({
        "import_type": basetype,
        import_type_array_map[basetype][0]: objects
    }, indent=4))
    f.close()
    manifest["payloads"].append(filename)
    print("saved basetype", basetype, "as", filename)
    return manifest
