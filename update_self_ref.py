#!/usr/bin/python3

import sqlite3
import easydb.server.api
import json

easydb_api = easydb.server.api.EasydbAPI('http://uni-wuerzburg.5.pf-berlin.de')
easydb_api.authenticate('root','admin')
destination_conn = sqlite3.connect("../destination/destination.db")
destination_c = destination_conn.cursor()
destination_c.execute('SELECT __source_unique_id, __easydb_id FROM "easydb.bilder"')
id_pairs=destination_c.fetchall()
for pair in id_pairs:
    destination_c.execute('UPDATE "easydb.bilder__bilder" SET __uplink_id = {} WHERE __uplink_id={}'.format(pair[1],pair[0]))
    destination_c.execute('UPDATE "easydb.bilder__bilder" SET bild_id = {} WHERE bild_id={}'.format(pair[1],pair[0]))
destination_conn.commit()

destination_c.execute('SELECT bb.__uplink_id, bb.bild_id, b.__pool_id, b.__easydb_goid, bb.remark FROM "easydb.bilder__bilder" bb JOIN "easydb.bilder" b on (b.__easydb_id=bb.__uplink_id)')
rows=destination_c.fetchall()

for row in rows:
    print(row)
    uplink = row[0]
    downlink = row[1]
    if uplink==downlink: continue
    pool_id = row[2]
    goid=row[3]
    get_url="db/bilder/bildedit/{}".format(uplink)
    remark=row[4]

    js_down=easydb_api.get(get_url)
    js_up=js_down
    if not '_nested:bilder__bilder' in js_up[0]['bilder'].keys():
        js_up[0]['bilder']['_nested:bilder__bilder']=[]
    js_up[0]['bilder']['_nested:bilder__bilder'].append({
               "bild_id": {
                  "_objecttype": "bilder",
                  "_mask": "bildedit",
                  "bilder": {
                     "_id": downlink
                  }
               },
               "remark": remark
            })
    js_up[0]["bilder"]["_version"]+=1
    #print(json.dumps(js_up,indent=4))
    post_url="db/bilder/"