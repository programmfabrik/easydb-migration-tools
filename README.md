# easydb-migration-tools

The easydb migration tools are used to migrate information from a given system to an easydb.

A migration consists of the following:

- Extract: extract information from the - possibly heterogeneous - data sources ("origin") to a normalized representation ("source")
- Transform: extract information from the "source" and transform it to its final representation ("destination")
- Load: load the information from the "destination" to an easydb

## Extract

The module easydb.migration.extract offers diverse extraction tools to build a "source", which is an SQLite database, and fill it
with the data gathered from different origin systems.
