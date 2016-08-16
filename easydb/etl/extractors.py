__all__ = [
    'ExtractList',
    'ExtractSQL'
]

from easydb.etl.extract import Extractor

class ExtractList(Extractor):
    def __init__(self, rows, name='ExtractList'):
        self.rows = rows
        self.name = name
    def extract(self):
        return self.rows
    def __str__(self):
        return self.name

class ExtractSQL(object):
    def __init__(
        self,
        source,
        sql,
        name='ExtractSQL'):
        self.source = source
        self.sql = sql
        self.name = name
    def extract(self):
        return self.source.execute(self.sql)
    def __str__(self):
        return self.name
