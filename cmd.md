-t --target SQLITE_FILE (required)
--init (optional, if true, purge sqlite file)
--name NAME (required, default: "source")
--dump-mysql If set, output in mysql sql format, use "-" to dump to STDOUT

== pg

--dsn "host=... user=... port=..."
--schema SCHEMA
--table TABLE

== mysql

--host
--db
--user
--password
--schema SCHEMA (optional)
--table TABLE (optional)

== file

--csv FILE
--xml FILE
--sqlite FILE

== easydb4

Creates necessary tables..

--config <login:password@url> /ezadmin/dumpconfig is automatically added

--dsn "host=... user=... port=..."
--sqlite <file>
--eas_url
--eas_instance
--eas_versions original:url 100:data


data2sqlite.py easydb4 --output source.db --name easydemo2 --init --config admin:admin@easydemo2
data2sqlite.py pg --output source.db --name lokando --dsn "host=lokando"
data2sqlite.py mysql --output source.db --name antares --db antares --user root
data2sqlite.py file --output source.db --name files --csv csv1.csv --xml ... --sqlite ...

# convert sqlite file to mysql and pipe to mysql
data2sqlite.py -t source.db --sqlite easydemo2.sqlite --dump-mysql - | mysql easydemo












