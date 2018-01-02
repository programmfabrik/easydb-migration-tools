#!/usr/bin/python3
import logging
import logging.config
import sys
import sqlite3


import easydb.migration.transform.job
import easydb.migration.transform.prepare
from easydb.migration.transform.extract import AssetColumn

#execution: ./transform eadb-url source-directory destination-directory --login LOGIN --password PASSWORD || requires source-db named "source.db" in source-directory
###############################################################################

##INSTANZSPEZIFISCHE VARIABLEN
##VOR AUSFÜHRUNG SETZEN!

schema= "public"                                #meistens 'public' Bei mehreren Schemata manuell für jeden Tabellen Eintrag festlegen
instanz= "source"                                #Instanzname in Postgres z.B. lette-verein, easy5-annegret o.ä.
collection_table="workfolder2"                         #Bezeichnung der Mappen-Tabelle in Source
collection_objects_table= "workfolder2_bilder"                  #Link-Tabelle für Objekte in Mappen

###############################################################################


if schema is None or instanz is None or collection_table is None or collection_objects_table is None:
    print('Instanzspezifische Variablen festlegen')
    sys.exit(0)

# setup
job = easydb.migration.transform.job.TransformJob.create_job('INSTANZNAME', easydb.migration.transform.prepare.CreatePolicy.IfNotExists)#creates transform-job named "INSTANZNAME" (change accordingly)


#logger-setup, doesnt have to be changed
standard_formatter = logging.Formatter('%(asctime)s %(levelname)s [%(name)s] %(message)s', '%Y.%m.%d %H:%M:%S')
user_formatter = logging.Formatter('%(message)s')
root_logger = logging.getLogger()
root_logger.setLevel('DEBUG')
user_logger = logging.getLogger('user')
user_logger.setLevel('DEBUG')

console_log = logging.StreamHandler()
console_log.setLevel(logging.DEBUG)
console_log.setFormatter(standard_formatter)
root_logger.addHandler(console_log)

migration_log = logging.FileHandler('{}/migration.log'.format(job.destination_dir))
migration_log.setLevel(logging.DEBUG)
migration_log.setFormatter(standard_formatter)
root_logger.addHandler(migration_log)

user_log = logging.FileHandler('{}/user.log'.format(job.destination_dir))
user_log.setLevel(logging.DEBUG)
user_log.setFormatter(user_formatter)
user_logger.addHandler(user_log)

logging.getLogger('easydb.server').setLevel('WARN')
logging.getLogger('requests').setLevel('WARN')
logging.getLogger('easydb.repository').setLevel('WARN')
logging.getLogger('easydb.migration.transform.source').setLevel('WARN')
logging.getLogger('easydb.migration.transform.prepare').setLevel('INFO')
logging.getLogger('easydb.migration.transform.extract').setLevel('INFO')

def final_touch(tables):
    source_conn = sqlite3.connect(job.source.filename)
    source_c = source_conn.cursor()
    destination_conn = sqlite3.connect(job.destination.filename)
    destination_c = destination_conn.cursor()

    destination_c.execute('DELETE FROM "easydb.ez_user" WHERE login="root"')#Delete root-user, to prevent conflicting unique_user constraint (root is default system-user)
    destination_c.execute('INSERT INTO "easydb.ez_pool" ("__source_unique_id", "name:de-DE") VALUES ("STANDARD", "STANDARD_FALLBACK")')#create FALLBACK-pool for any records that have no pool

    for table in tables:

        if table['has_parent']:
            req = 'SELECT fk_father_id, id FROM "' + table["table_from"] +'"'#get parent-ids from source
            for row in source_c.execute(req):
                if row[0]!=None:
                    write = 'UPDATE "{0}" SET __parent_id = '.format(table["table_to"]) + str(row[0]) + ' WHERE __source_unique_id = ' + str(row[1])#set parent-id for lists with hierarchical-ordering
                else:
                    write = 'UPDATE "{0}" SET __parent_id = NULL'.format(table["table_to"]) + ' WHERE __source_unique_id = ' + str(row[1])#set no parent-id
                destination_c.execute(write)
        if table['has_pool']:
            destination_c.execute('UPDATE "{0}" SET __pool_id ="STANDARD" WHERE __pool_id is NULL'.format(table["table_to"]))#set pool-id for records that are supposed to be organized in pool, but have no pool assigned
        if table['objects_table'] is not None:
            destination_c.execute('SELECT object_id, collection_id FROM "easydb.ez_collection__objects"')
            rows = destination_c.fetchall()
            for row in rows:
                query='UPDATE "{0}" SET collection_id = {1} WHERE __source_unique_id = {2}'.format(table["objects_table"], row[1], row[0])
                destination_c.execute(query)
    destination_conn.commit()

#create destination.db
job.prepare()
# Wemm nur eine leere Destion erzeugt werden soll: nächste Zeile aktivieren
exit()

# transform
tables=[]       #list of all tables, a transformation for each table must be appended in the dictionary stile below

##USERS UNION MIGHT CAUS TROUBLE BECAUSE OF IDENTICAL IDS IN DIFFERENT TABLES SHOULD BE DISABLED BY DEFAULT; BUT MIGHT COME IN HANDY FOR LDAP/KERBEROS OR SUCH
tables.append(
    {
        'sql':
        """\
        SELECT
            id as __source_unique_id,
            login,
            email,
            vorname as first_name,
            nachname as last_name,
            password as hashed_password
        FROM "{0}.{1}.user"
        """.format(instanz,schema),
        'table_from': '{}.{}.user'.format(instanz,schema),
        'table_to': 'easydb.ez_user',
        'has_parent': False,
        'has_pool': False,
        'has_asset': False,
        'objects_table': None
    }
)
##GROUPS
tables.append(
    {
       'sql':
       """\
        SELECT
            id as __source_unique_id,
            name,
            name as "displayname:de-DE"
        FROM "{}.{}.usergruppe"
        """.format(instanz,schema),
        'table_from':'{}.{}.usergruppe'.format(instanz,schema),
        'table_to':'easydb.ez_group',
        'has_parent': False,
        'has_pool': False,
        'has_asset': False,
        'objects_table': None
    }
)

##GROUP-USERS
tables.append(
    {
        'sql':
        """\
        SELECT
            id as __source_unique_id,
            to_id as group_id,
            from_id as user_id
        FROM "{0}.{1}.eadb_links"
        WHERE from_table_id=14 and to_table_id=12
        """.format(instanz,schema),
        'table_from': '{0}.{1}.eadb_links'.format(instanz, schema),      #table in source
        'table_to': 'easydb.ez_user__group',                                 #table in destination
        'has_parent': False,                                        #True if Object is part of a List with hierarchical ordering
        'has_pool': False,                                          #True if records of this table are orgranized in pools
        'has_asset': False,                                          #True if record has a file attached to it
        'asset_columns': [AssetColumn(instanz, '{}.main'.format(schema), 'bild', 'main', 'bild', ['url'])],
        'objects_table': None
    }
)

##POOLS
tables.append(
    {
        'sql':
        """\
        SELECT
            id as __source_unique_id,
            name as "name:de-DE"
        FROM "{}.{}.pool"
        """.format(instanz,schema),
        'table_from':'{}.{}.pool'.format(instanz,schema),
        'table_to':'easydb.ez_pool',
        'has_parent': True,
        'has_pool': False,
        'has_asset': False,
        'objects_table': None
   }
)

##COLLECTIONS
tables.append(
    {
        'sql':
        """\
        SELECT
            id as __source_unique_id,
            "collection" as __type,
            fk_father_id as __parent_id,
            name as "displayname:de-DE",
            easydb_owner as __owner
        FROM "{}.{}.{}"
        JOIN "{0}.{1}.user" u ON c.easydb_owner = 'user_' || u.login
        """.format(instanz,schema,collection_table),
        'table_from':'{}.{}.{}'.format(instanz,schema,collection_table),
        'table_to':'easydb.ez_collection',
        'has_parent': False,
        'has_pool': False,
        'has_asset': False,
        'objects_table': None
   }
)

##PRESENTATIONS
tables.append(
    {
        'sql':
        """\
        SELECT
            id + (SELECT MAX(id) FROM "{0}.{1}.{2}") as __source_unique_id,
            coalesce(name,id) as "displayname:de-DE",
            easydb_owner as __owner,
            'presentation' as __type
        FROM "{0}.{1}.presentation"
        JOIN "{0}.{1}.user" u ON p.easydb_owner = 'user_' || u.login
        """.format(instanz,schema,collection_table),
        'table_from':'{}.{}.presentation'.format(instanz,schema),
        'table_to':'easydb.ez_collection',
        'has_parent': False,
        'has_pool': False,
        'has_asset': False,
        'objects_table': None
   }
)
################################################################################
#-------------------->INSERT CUSTOM OBJECT-TYPES HERE<---------------------------
##INDIVDUAL TABLES: MUST BE CHANGED TO FIT ACTUAL VALUES
##Example with Assets, linked Objects using eadb_links in easydb4 and organized in Pools, as well as Collections
##Main is the table of actual objects and linked the table of linked objects
tables.append(
    {
        'sql':
        """\
        SELECT
            id as __source_unique_id,
            name,
            date_from || "|" || date_to as daterange,
            diverse_andere_felder
        FROM "{0}.{1}.main"
        """.format(instanz,schema),                                 #sql query (hard to automatize, because of varying join, etc.), all fields are examples, must replace those
        'table_from': '{0}.{1}.main'.format(instanz, schema),      #table in source
        'table_to': 'easydb.main',                                 #table in destination
        'has_parent': False,                                        #True if Object is part of a List with hierarchical ordering
        'has_pool': True,                                          #True if records of this table are orgranized in pools
        'has_asset': True,                                          #True if record has a file attached to it
        'asset_columns': [AssetColumn(instanz, '{}.main'.format(schema), 'bild', 'main', 'bild', ['url'])],
        'objects_table': None
    }
)
##Example using eadblinks, from and to_table_id can be retrieved from eadb_tables
tables.append(
    {
        'sql':
        """\
		select
		    to_id as main_id,
		    from_id || ':' || to_id as '__source_unique_id',
		    from_id as __uplink_id
		from "{}.{}.eadb_links" l
		join "{}.{}.main" b on (l.from_id = b.id)
		join "{}.{}.linked" g on (l.to_id = g.id)
		where to_table_id=58 AND from_table_id=1
		""".format(instanz,schema),                                 #get table_ids from eadb_tables in Source.
        'table_from': '{0}.{1}.linked'.format(instanz, schema),      #table in source
        'table_to': 'easydb.linked',                                 #table in destination
        'has_parent': False,                                        #True if Object is part of a List with hierarchical ordering
        'has_pool': False,                                          #True if records of this table are orgranized in pools
        'has_asset': False,                                          #True if record has a file attached to it
        'objects_table': None
    }
)

################################################################################
##COLLECTION OBJECTS IN EASYDB 4 ENTWEDER EIGENE TABLE ODER EADB_LINKS
tables.append(
    {
        'sql':
        """\
        SELECT
            id as __source_unique_id,
            lk_bild_id as object_id,
            lk_arbeitsmappe_id as collection_id
		FROM "{}.{}.{}"
        """.format(instanz,schema,collection_objects_table),
        'table_from':'{}.{}.{}'.format(instanz,schema,collection_objects_table),
        'table_to':'easydb.ez_collection__objects',
        'has_parent': False,
        'has_pool': False,
        'has_asset': False,
        'objects_table': 'easydb.main' #Table of Objects in workfolder in destination
    }
)
##PRESENTATION OBJECTS
#USUALLY STORED IN EADB-LINKS, BUT MIGHT BE STORED IN SPERATE TABLE
tables.append(
    {
        'sql':
        """\
        SELECT
            id as __source_unique_id,
            to_id as object_id,
            from_id + (SELECT MAX(id) FROM "{0}.{1}.{2}") as collection_id,
            position
		FROM "{0}.{1}.eadb_links"
        WHERE from_table_id=24 and to_table_id=1
        """.format(instanz,schema,collection_table),
        'table_from':'{}.{}.{}'.format(instanz,schema,collection_objects_table),
        'table_to':'easydb.ez_collection__objects',
        'has_parent': False,
        'has_pool': False,
        'has_asset': False,
        'objects_table': 'easydb.assets' #Table of Objects in workfolder in destination
    }
)

for table in tables:
    if table['has_asset']:#Write records with files attached
        job.extract_sql(table['sql'], table['table_to'], asset_columns=table['asset_columns'])

    else:#write assets with no file
        job.extract_sql(table['sql'], table['table_to'])

final_touch(tables)
job.log_times()
