#!/usr/bin/python3
# coding=utf8

import os
import json
import argparse
import migration_util

argparser = argparse.ArgumentParser(description='change batch sizes')

argparser.add_argument('sourcefolder', help='Folder with JSON files')
argparser.add_argument('-tf', '--targetfolder', default='',
                       help='Target Folder for new JSON files, if not set the source is used')
MAX_BATCH_SIZE = 1000
argparser.add_argument('-bs', '--new_batchsize', default=str(MAX_BATCH_SIZE),
                       help='New JSON File Batch Size - max: ' + str(MAX_BATCH_SIZE))

args = argparser.parse_args()

batch_size = int(args.new_batchsize)
if batch_size < 0:
    batch_size = 0
if batch_size > MAX_BATCH_SIZE:

    exit()

SOURCE_FOLDER = args.sourcefolder + "/"
TARGET_FOLDER = args.targetfolder + \
    "/" if len(args.targetfolder) > 0 else SOURCE_FOLDER

source = ""
payload_base_uri = ""
payloads = None
try:
    mf = json.loads(open(os.path.abspath(
        SOURCE_FOLDER + "manifest.json")).read())

    if "source" in mf:
        source = mf["source"]
    if "payload_base_uri" in mf:
        payload_base_uri = mf["payload_base_uri"]
    if "payloads" in mf:
        payloads = mf["payloads"]

except Exception as e:


manifest = {
    "source": source,
    "batch_size": batch_size,
    "payload_base_uri": payload_base_uri,
    "payloads": []
}

old_batches = {}
new_batches = {}

for subdir, dirs, files in os.walk(SOURCE_FOLDER):
    subdir_path = subdir.split('/')


    file_array = files if payloads is None else payloads

    for f in file_array:
        if f == "manifest.json":
            continue
        try:
            if f.split(".")[1].lower() not in ["json"]:
                continue
        except:
            continue


        try:
            pl = json.loads(open(os.path.abspath(SOURCE_FOLDER + f)).read())

            if "import_type" not in pl:

                continue
            import_type = pl["import_type"]

            if not import_type in migration_util.import_type_array_map:

                continue

            array_name = migration_util.import_type_array_map[import_type]


            objecttype = None
            if import_type == "db":
                if not "objecttype" in pl:

                          "requires field objecttype, skip")
                    continue
                objecttype = pl["objecttype"]



            if not array_name in pl:

                continue


            if import_type not in old_batches:
                old_batches[import_type] = {}

            if array_name not in old_batches[import_type]:
                old_batches[import_type][array_name] = {}
                if objecttype is None:
                    old_batches[import_type][array_name]["data"] = []
                else:
                    if objecttype not in old_batches[import_type][array_name]:
                        old_batches[import_type][array_name][objecttype] = []

            for obj in pl[array_name]:
                if objecttype is None:
                    old_batches[import_type][array_name]["data"].append(obj)
                else:
                    old_batches[import_type][array_name][objecttype].append(
                        obj)

        except Exception as e:




    for import_type in old_batches:



        if import_type not in new_batches:
            new_batches[import_type] = {}

        for array_name in old_batches[import_type]:

            if array_name not in new_batches[import_type]:
                new_batches[import_type][array_name] = {}

            for data in old_batches[import_type][array_name]:

                new_batches[import_type][array_name][data] = []

                n = 0
                current_batch_size = 0
                batch_number = 0
                batch_name = None
                for ob in old_batches[import_type][array_name][data]:

                    if current_batch_size == 0:
                        batch_name = "%s_batch_%04d_" % (data, batch_number)


                        new_batches[import_type][array_name][data].append({
                            batch_name: []
                        })

                    if batch_name is not None:
                        new_batches[import_type][array_name][data][-1][batch_name].append(
                            ob)

                    n += 1
                    current_batch_size += 1

                    if current_batch_size >= batch_size:
                        current_batch_size = 0
                        batch_number += 1



    for import_type in new_batches:



        for array_name in new_batches[import_type]:


            for data in new_batches[import_type][array_name]:
                for ar in new_batches[import_type][array_name][data]:
                    for key in ar:
                        if isinstance(ar[key], list):
                            if len(ar[key]) > 0:

                                manifest = migration_util.save_batch(
                                    ar[key],
                                    TARGET_FOLDER,
                                    "%s%04d.json" % (key, len(ar[key])),
                                    import_type,
                                    manifest,
                                    data if import_type == "db" else None)
                        break

    break

manifest_file = open(TARGET_FOLDER + "manifest.json", 'w')
manifest_file.write(json.dumps(manifest, indent=2))
manifest_file.close()

