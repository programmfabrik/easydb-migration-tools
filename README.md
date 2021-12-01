# easydb-migration-tools

The easydb migration tools are used to migrate information from a given system to an easydb.

A migration consists of the following:

- Extract: extract information from the - possibly heterogeneous - data sources ("origin") to a normalized representation ("source")
- Transform: extract information from the "source" and transform it to its final representation ("destination")
- Load: load the information from the "destination" to an easydb

## Extract

The module easydb.migration.extract offers diverse extraction tools to build a "source", which is an SQLite database, and fill it
with the data gathered from different origin systems.

## Transform

The module easydb.migration.transform offers diverse extraction and transformation tools to build a "destination",
which is an SQLite database, and fill it with the data processed from the "source".

## data2sqlite

Script to convert different file sources to a sqlite3 database file.

See [Documentation](data2sqlite.md)

## json_migration

Collection of tools that help with generating payloads for the json importer (https://docs.easydb.de/en/tutorials/jsonimport).

See [Generated Code Documentation](doku/json_migration/index.md)
