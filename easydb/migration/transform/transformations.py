__all__ = [
    'FixValues',
    'FilterByEmptyColumns',
    'FilterBySourceId'
]

from easydb.migration.transform.extract import RowTransformation

class FixValues(RowTransformation):
    def __init__(self, fix_values):
        self.fix_values = fix_values
    def __str__(self):
        return 'fix-values'
    def transform(self, row):
        for key, value in self.fix_values.items():
            row[key] = value
        return row

class FilterByEmptyColumns(RowTransformation):
    def __init__(self, columns=['__source_unique_id']):
        self.columns = columns
    def __str__(self):
        return 'filter-empty-columns'
    def transform(self, row):
        for column in self.columns:
            if row[column] is not None and len(row[column].strip()) > 0:
                break
        else:
            return None
        return row

class FilterBySourceId(RowTransformation):
    def __init__(self, exclude_ids):
        self.exclude_ids = exclude_ids
    def __str__(self):
        return 'filter-by-source-id'
    def transform(self, row):
        source_id = row['__source_unique_id']
        if source_id not in self.exclude_ids:
            return row
        else:
            return None
