# Migration von easydb4 auf easydb5
## Grundlegend
Für die Migration werden zwei sqlite-Datenbanken erstellt. Zunächst werden die Daten von easydb4 Server
extrahiert und in einer Datenbank gespeichert, die "source.db" genannt werden muss. Diese ist im Weiteren gemeint,
wenn die Rede von "Source" ist.
Die Daten werden transformiert und in einer weiteren Datenbank Namens "destination.db" abgelegt, die im Weiteren als "Destination"
bezeichnet wird.
## Extraction
Um die Source-Datenbank zu erstellen muss der gesamte Inhalt des Git-Repos "easydb-tools-migration" auf den easydb4 Server geklont oder kopiert werden.
Für die Erstellung von Source ist das Python-Skript "extraction.py" relevant. Hier müssen zunächst die Variablen "schema", "eas-instanz" und "instanz" gesetzt werden.
Diese lassen sich am einfachsten über die "/ezadmin" Seite der easydb4 herausfinden. Alle weiteren Werte wie Verzeichnisse, etc. sind im Extraction-Skript
auf übliche Werte gesetzt, müssen gegebenenfalls aber noch angepasst werden.
In üblicher Konfiguration bezieht das Extraction-Skript strukturelle Information zu Tabellen und Schemas aus einer sqlite-Datenbank in "/opt/easydb/4.0/sql/sqlite/", Daten aus der Postgres-DB der easydb4 und links zu den zughörigen Assets vom EAS-Server. Weitere Quellen sind jedoch ebenfalls möglich. Das Skript "easydb/migration/extract.py" bietet dafür Methoden für unterschiedliche Quellen.
Sind alle Instanzspezifischen Variablen angepasst, kann das Skript im Verzeichnis "easydb-tools-migration/easydb/" mit

> ./extraction.py source/source.db

ausgeführt werden. Dies erzeugt Source im Unterverzeichnis "source". Dieses Verzeichnis muss vorher angelegt werden.
Die Tabellen in Source werdne nach folgendem Prinzip benannt

>instanz.schema.tabellen-name

## Transformation
Für den Upload der Daten müssen diese in ein zur Struktur der easydb5 kompatible Form gebracht (transformiert) werden. Zu diesem Zweck wird das Skrip "transformation.py" benötigt.
Wie bei der Extraktion müssen zunächst instanzspezifische Variablen ausgefüllt werden.
"instanz" und "schema" bleiben gleich wie beim ersten Schritt. Für die Variablen "collection_table" und
"collection_objects_table" muss in Source geschaut werden. "collection_table" muss auf den Namen der Mappen-Tabelle ("arbeitsmappen", "workfolders", o.ä.) und "collection_objects_table" auf den Namen der dazugehörigen Link-Tabelle  ("arbeitsmappe\__bilder", "workfolder\__assets", o.ä.) gesetzt werden.
Um ersichtlich zu machen, welche Transformationen nötig sind, sollte zunächst eine leere Destination aus dem Datenmodell der easydb5 erstellt werden. Zu diesem Zweck sollte im Transformation-Skript alles ab Zeile 66 auskommentiert werden und das Skript dann mit

>./transform eadb-url source-directory destination-directory --login LOGIN --password PASSWORD

ausgeführt werden (am besten den easydb-root-user verwenden, das destination-directory muss vorher angelegt weren).

Alle Tabellen der easydb4, die Migriert werden sollen, müssen mit einem Dictionary im Transformation-Skript
beschrieben werdeb und dieses der Liste "tables" angehängt werden. Jedes Dictionary muss dabei folgende Form haben

```python
{
    'sql':
    """\
    SELECT
        id as __source_unique_id,
        name,
        name as "displayname:de-DE"
    FROM "{0}.{1}.table_from"
    """.format(instanz,schema),                                 #sql query (hard to automatize, because of varying join, etc.), all fields are examples, must replace those
    'table_from': '{0}.{1}.table'.format(instanz, schema),      #table in source
    'table_to': 'easydb.table',                                 #table in destination
    'has_parent': False,                                        #True if Object is part of a List with hierarchical ordering
    'has_pool': False,                                          #True if records of this table are orgranized in pools
    'has_asset': False                                          #True if record has a file attached to it
    'asset_columns': [AssetColumn(instanz, '{}.table'.format(schema), 'column', 'table', 'column', ['url'])]
}
```
* Im Key 'sql' wird die SQL-Query abgelegt, die Daten aus Source holt und in das Format der Destination bringt (sourc_column as destination_column).
* Die Key "table_from" und "table_to" enthalten den Tabellen-Namen in Source, bzw. Destination.
* "has_parent" muss True sein für alle Datensätze, die hierachisch organisiert sind.
* "has_pool" muss True sein für alle Datensätze, die in Pools organisiert sind.
* "has_asset" muss True sein für alle Datensätze, die Assets besitzen. Für diese muss auch der key
* "assett_collumns" gesetzt sein.

Für User, Gruppen, Pools und Mappen sind beriets Dictionaries definiert. Alles weitere muss händisch hinzugfügt werden. Wenn alle Tabellen mit einem Dictionary beschrieben  und dieses der Liste "tables" angehängt wurde, kann die Transformation mit

>./transform eadb-url source-directory destination-directory --login LOGIN --password PASSWORD

gestartet werden. Vorher muss der zuvor auskommentierte Code wieder ausführbar gemacht werden.

## Upload

Um die Daten von Destination ins Zielsystem zu übertragen, muss das Uplaod-Skript "upload.py" ausgeführt werden. Dies kann unter Umständen sehr lange dauern. Es ist daher ratsam alle bisher erzeugten Daten und das "easydb-tools-migration"-Verzeichnis auf das Zielsystem zu übertragen.
Die Anpassungen im Upload-Skript sind minimal. Es müssen lediglich der Liste "objecttypes" die Tabellen hinzugefügt werden, die in die easydb5 übertragen werden sollen. Ausgeschlossen sind Link-Tabellen (in Destination habe diese die Form "Datensatz__nested-datensatz"). Standardmäßig sind bereits die Tabellen für Benutzer, Gruppen, Arbeitsmappen  und Pools berücksichtigt.

Wenn alle Objekttypen vermerkt sind, kann der uplaod mit

>./upload.py http://localhost http://localhost/eas eas-instanz source/ destination/ --login LOGIN --password PASSWORD

auf dem Zielsystem gestartet werden (ggF. müssen Verzeichnisse und URLs angepasst werden). Es ist ratsam diesen Aufruf in einem Screen zu starten, damit das Skript auch bei geschlossener SSH-Session weiterlaufen kann.
