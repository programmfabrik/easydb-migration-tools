# migration_util module


### class migration_util.ObjectPayloadManager(verbose)
Bases: `object`


#### \__init__(verbose)
__init__ Constructor


* **Parameters**

    **verbose** (*bool*) – debug?



#### classmethod empty_db_payload(objecttype)
empty_db_payload wrapper with empty payload with import_type ‘db’


* **Parameters**

    **objecttype** (*str*) – objecttype



* **Returns**

    payload object



* **Return type**

    dict



#### classmethod empty_payload(import_type, obj_key='objects')
empty_payload create an empty payload object


* **Parameters**

    
    * **import_type** (*str*) – value for import_type key


    * **obj_key** (*str**, **optional*) – key for array with objects, defaults to ‘objects’



* **Returns**

    payload object



* **Return type**

    dict



#### merge_export_object(objecttype, ref_col, obj)
merge_export_object add the object to the map of export object, merge if necessary


* **Parameters**

    
    * **objecttype** (*str*) – objecttype


    * **ref_col** (*str*) – name of the reference column, must be in the object


    * **obj** (*dict*) – object with new data



* **Returns**

    added number of objects: 1 of a new object was added, else 0



* **Return type**

    int



#### classmethod merge_object(new_obj, old_obj, top_level=True)
merge_object helper method to merge two objects with the same reference and different keys


* **Parameters**

    
    * **new_obj** (*dict*) – object with new data


    * **old_obj** (*dict*) – old object


    * **top_level** (*bool**, **optional*) – for recursive call of function, defaults to True



* **Raises**

    **Exception** – Exception with information if merging failed



* **Returns**

    the merged object



* **Return type**

    dict



#### classmethod save_batch(payload, outputfolder, filename, import_type, manifest, objecttype=None)
save_batch save the batch of objects/basetypes as json files


* **Parameters**

    
    * **payload** (*list*) – payload


    * **outputfolder** (*str*) – target folder for json files


    * **filename** (*str*) – filename of json file


    * **import_type** (*str*) – value for import_type key


    * **manifest** (*dict*) – manifest


    * **objecttype** (*str**, **optional*) – objecttype if the payload does not contain basetypes, defaults to None



* **Returns**

    manifest



* **Return type**

    dict



#### save_payloads(manifest, outputfolder, objecttype, ref_col, batchsize)
save_payloads save objects as json files, add payload names to manifest


* **Parameters**

    
    * **manifest** (*dict*) – manifest


    * **outputfolder** (*str*) – target folder for json files


    * **objecttype** (*str*) – objecttype


    * **ref_col** (*str*) – name of the reference column, must be in the object


    * **batchsize** (*int*) – maximal number of objects per json file



* **Returns**

    manifest



* **Return type**

    dict



### migration_util.append_to_logfile(logfile, s, timestamp=None)
append_to_logfile append line with timestamp to the logfile


* **Parameters**

    
    * **logfile** (*str*) – filename of logfile


    * **s** (*str*) – line with log message


    * **timestamp** (*datetime**, **optional*) – timestamp, defaults to None, will then be set to datetime.now()



### migration_util.check_image_url_reachable(url, verbose=False)
check_image_url_reachable check if the given URL is reachable


* **Parameters**

    
    * **url** (*str*) – URL


    * **verbose** (*bool**, **optional*) – debug?, defaults to False



* **Returns**

    if the URL is reachable



* **Return type**

    bool



### migration_util.datetime_to_date(d)
datetime_to_iso format datetime object with easydb5 iso format ‘%Y-%m-%d’


* **Parameters**

    **d** (*datetime*) – datetime object



* **Returns**

    formatted string



* **Return type**

    str



### migration_util.datetime_to_iso(d)
datetime_to_iso format datetime object with easydb5 iso format ‘%Y-%m-%dT%H:%M:%S’


* **Parameters**

    **d** (*datetime*) – datetime object



* **Returns**

    formatted string



* **Return type**

    str



### migration_util.dumpjs(d, indent=4)
dumpjs convert dict to a pretty printed json string


* **Parameters**

    
    * **d** (*dict*) – dict


    * **indent** (*int**, **optional*) – number of spaces for indent, defaults to 4



* **Returns**

    pretty printed json string



* **Return type**

    str



### migration_util.format_date_object(year, month, day)
format_date_object convert year, month, day strings to an easydb5 date object


* **Parameters**

    
    * **year** (*str*) – year


    * **month** (*str*) – month, can be None


    * **day** (*str*) – day, can be None



* **Returns**

    easydb5 date object



* **Return type**

    dict



### migration_util.format_string_list(strings)
format_string_list join values with spaces


* **Parameters**

    **strings** (*list*) – list of values



* **Returns**

    string



* **Return type**

    string



### migration_util.generate_hash_reference(value)
generate_hash_reference convert value to md5 checksum


* **Parameters**

    **value** (*Any*) – value



* **Returns**

    string with md5 checksum in hex format



* **Return type**

    stri



### migration_util.init_error_log()
init_error_log call init_logfile for error log file


### migration_util.init_info_log()
init_error_log call init_logfile for info log file


### migration_util.init_logfile(logfile)
init_logfile create empty logfile


* **Parameters**

    **logfile** (*str*) – filename of logfile



### migration_util.log_error(\*strings)
log_error append values as new line to error log file


### migration_util.log_info(\*strings)
log_error append values as new line to info log file


### migration_util.percentage(n, total)
percentage string with calculated percentage between n and total


* **Parameters**

    
    * **n** (*int*) – number of objects


    * **total** (*int*) – total number of objects



* **Returns**

    formatted string



* **Return type**

    string



### migration_util.print_traceback(e)
print_traceback print traceback of given exception


* **Parameters**

    **e** (*Exception*) – Exception



* **Returns**

    traceback



* **Return type**

    list of strings



### migration_util.sqlite3_connect(filename, detect_types=1)
sqlite3_connect connect to sqlite3 file


* **Parameters**

    
    * **filename** (*str*) – sqlite3 file


    * **detect_types** (*int**, **optional*) – detect types, defaults to sqlite3.PARSE_DECLTYPES



* **Raises**

    **e** – sqlite3.OperationalError



* **Returns**

    connection



* **Return type**

    sqlite3.Connection



### migration_util.sqlite3_count_rows(con, table, specific_select=None, debug=False)
sqlite3_count_rows count rows in a table, can be expanded to use a WHERE clause


* **Parameters**

    
    * **con** (*sqlite3.Connection*) – connection to sqlite3 database


    * **table** (*str*) – table name


    * **specific_select** (*str**, **optional*) – WHERE clause that is used in the SELECT statement, defaults to None


    * **debug** (*bool**, **optional*) – debug?, defaults to False



* **Returns**

    number of rows



* **Return type**

    int



### migration_util.sqlite3_select(con, query, params=[], debug=False)
sqlite3_select perform a SELECT statement


* **Parameters**

    
    * **con** (*sqlite3.Connection*) – connection to sqlite3 database


    * **query** (*str*) – SQL query with SELECT statement


    * **params** (*list**, **optional*) – list of parameters, defaults to []


    * **debug** (*bool**, **optional*) – debug?, defaults to False



* **Returns**

    list of rows, each row is dict with column names as keys



* **Return type**

    list



### migration_util.time_per_object(start, n)
time_per_object string with calculated average time to generate one object


* **Parameters**

    
    * **start** (*datetime*) – timestamp of start of generating objects


    * **n** (*int*) – total number of objects



* **Returns**

    formatted string



* **Return type**

    string



### migration_util.to_easydb_date_object(d)
to_easydb_date_object wrapper for a easydb5 date object


* **Parameters**

    **d** (*str*) – date string



* **Returns**

    easydb5 date object



* **Return type**

    dict
