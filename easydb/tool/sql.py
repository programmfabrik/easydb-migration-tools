'''
* easydb-migration-tools - easydb Migration Tools
 * Copyright (c) 2013 - 2016 Programmfabrik GmbH
 * MIT Licence
 * https://github.com/programmfabrik/coffeescript-ui, http://www.coffeescript-ui.org
'''

__all__ = [
    'sql_select',
    'sql_list'
]

def sql_select(table, columns):
    for f in ['__source_unique_id']:
        for name, alias in columns.items():
            if isinstance(alias, list):
                if f in alias:
                    break
            elif alias == f:
                break
        else:
            if f in columns:
                if not isinstance(columns[f], list):
                    columns[f] = [columns[f]]
            else:
                columns[f] = []
            columns[f].append(f)
    select_parts = []
    for name, alias in columns.items():
        if not isinstance(alias, list):
            alias = [alias]
        for a in alias:
            select_parts.append('"{0}" as "{1}"'.format(name, a))
    return 'select {0} from [[table:{1}]]'.format(', '.join(select_parts), table)

def sql_list(l):
    return ', '.join(map(lambda x: "'{0}'".format(x.replace("'", "''") if isinstance(x, str) else x), l))
