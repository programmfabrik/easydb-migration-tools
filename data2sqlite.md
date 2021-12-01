# data2sqlite.py

```
usage: python3 data2sqlite.py [-h] [-t TARGET] [--init] [--name NAME] [--dump_mysql DUMP_MYSQL] [-s] {easydb4,pg,mysql,file,adhh} ...

data2qlite

positional arguments:
  {easydb4,pg,mysql,file,adhh}
                        Set Datasources
    easydb4             Set Migration Mode to create Source for Migration
    pg                  Add to Source from postgres
    mysql               Add to Source from mySQL
    file                Add to Source from other files
    adhh                Add to Source from ADHH XML files

optional arguments:
  -h, --help            show this help message and exit
  -t TARGET, --target TARGET
                        Sqlite-Dump name and directory (Default: ./dump.db)
  --init                If set, existing files will be purged
  --name NAME           NAME in target db (required, default: "source")
  --dump_mysql DUMP_MYSQL
                        If set, output in mysql sql format, use "-" to dump to STDOUT
  -s, --silent          If set, don't output progress every 100 rows.

```

## Datasources

### easydb4

Creates necessary tables.

```
usage: python3 data2sqlite.py easydb4 [-h] [--config CONFIG CONFIG CONFIG] [--pg_dsn PG_DSN] [--sqlite_file SQLITE_FILE] [--eas_url EAS_URL] [--eas_instance EAS_INSTANCE] [--eas_versions [EAS_VERSIONS [EAS_VERSIONS ...]]]
                              [--schema SCHEMA]

optional arguments:
  -h, --help            show this help message and exit
  --config CONFIG CONFIG CONFIG
                        Fetch Server Information from URL, usage: "--config URL login password" If set, no other arguments need to be set.
  --pg_dsn PG_DSN       DSN for easydb-PostgreSQL Server, must be sperated by spaces
  --sqlite_file SQLITE_FILE
                        Filename for easydb_SQLite Database
  --eas_url EAS_URL     URL for easydb-EAS-Server
  --eas_instance EAS_INSTANCE
                        Instance-Name on EAS-Server
  --eas_versions [EAS_VERSIONS [EAS_VERSIONS ...]]
                        Asset Version and Storage-Method, enter "version:method", e.g "original:url"
  --schema SCHEMA       Schema for pg-database, default = "public". Set to "none" to not use a schema.
```

#### Examples:

```
python3 data2sqlite.py easydb4 --output source.db --name easydemo2 --init --config admin:admin@easydemo2

python3 data2sqlite.py pg --output source.db --name lokando --dsn "host=lokando"

python3 data2sqlite.py mysql --output source.db --name antares --db antares --user root

python3 data2sqlite.py file --output source.db --name files --csv csv1.csv --xml ... --sqlite ...

```

### pg

```
usage: python3 data2sqlite.py pg [-h] [--dsn DSN] [--schema SCHEMA] [--tables [TABLES [TABLES ...]]]

optional arguments:
  -h, --help            show this help message and exit
  --dsn DSN             DSN for PostgreSQL,format: "dbname=easydb port=5432 user=postgres"
  --schema SCHEMA       Schema for pg-database, default = "public"
  --tables [TABLES [TABLES ...]]
                        Select Tables for Export from postgresql
```

### mysql

```
usage: python3 data2sqlite.py mysql [-h] [--host HOST] [--dbname DBNAME] [--username USERNAME] [--password PASSWORD] [--tables [TABLES [TABLES ...]]]

optional arguments:
  -h, --help            show this help message and exit
  --host HOST           mySQL host
  --dbname DBNAME       DB in mySQL host
  --username USERNAME   Username for mySQL-DB
  --password PASSWORD   PW for mySQL-User
  --tables [TABLES [TABLES ...]]
                        Select Tables for Export from postgresql
```

### file

```
usage: python3 data2sqlite.py file [-h] [--sqlite [SQLITE [SQLITE ...]]] [--XML [XML [XML ...]]] [--CSV [CSV [CSV ...]]] [--XLSX [XLSX [XLSX ...]]]

optional arguments:
  -h, --help            show this help message and exit
  --sqlite [SQLITE [SQLITE ...]]
                        Filename for SQLite Database
  --XML [XML [XML ...]]
                        Filename(s) for XML
  --CSV [CSV [CSV ...]]
                        Filename(s) for CSV
  --XLSX [XLSX [XLSX ...]]
                        Filename(s) for Excel (Supported formats are .xlsx, .xlsm, .xltx, .xltm)
```

### adhh

Special handling of ADHH file format

```
usage: python3 data2sqlite.py adhh [-h] [--sqlite [SQLITE [SQLITE ...]]] [--xml [XML [XML ...]]]

optional arguments:
  -h, --help            show this help message and exit
  --sqlite [SQLITE [SQLITE ...]]
                        Filename for SQLite Database
  --xml [XML [XML ...]]
                        Filename(s) for ADHH XML
```

## convert sqlite file to mysql and pipe to mysql

```
python3 data2sqlite.py -t source.db --sqlite easydemo2.sqlite --dump-mysql - | mysql easydemo
```












