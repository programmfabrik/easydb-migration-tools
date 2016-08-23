import abc

def check_open(f):
    def new_f(self, *args, **kwargs):
        if not self.is_open():
            raise Exception('Repository is not open')
        return f(self, *args, **kwargs)
    return new_f

class Repository(object):
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def open(self):
        """Open the repository"""
        return

    @abc.abstractmethod
    @check_open
    def close(self):
        """Close the repository"""
        return

    @abc.abstractmethod
    def is_open(self):
        """Check if repository is open"""
        return

    @abc.abstractmethod
    @check_open
    def get_schema_def(self):
        """Retrieve the SchemaDefinition for the repository"""
        return

    @abc.abstractmethod
    @check_open
    def create_schema(self, schema_def):
        """Create schema from definition"""
        for table_def in schema_def.tables:
            self.add_table(table_def)

    @abc.abstractmethod
    @check_open
    def extract_table(self, table_name):
        """Retrieve an iterator to the rows of the table. Each row is a map: column_name -> value"""
        return

    @abc.abstractmethod
    @check_open
    def add_table(self, table_def):
        """Add a table to the repository"""
        return

    @abc.abstractmethod
    @check_open
    def insert_row(self, table_name, row):
        """Insert a row into the repository. The row is a map: column_name -> value"""
        return

    @abc.abstractmethod
    @check_open
    def update_row(self, table_name, row, condition):
        """Update a row into the repository. The row is a map: column_name -> value. The condition is SQL"""
        return

    @abc.abstractmethod
    @check_open
    def execute(self, sql):
        """Execute an SQL statement and return an iterator"""
        return

    def __del__(self):
        if self.is_open():
            self.close()

class SchemaDefinition(object):
    def __init__(self, name):
        self.name = name
        self.tables = []
    def __repr__(self):
        return 'SCHEMA {0}\n\n{1}'.format(self.name, '\n'.join(map(repr, self.tables)))

class TableDefinition(object):
    def __init__(self, name):
        self.name = name
        self.columns = []
        self.constraints = []
    def __repr__(self):
        return '{0}\n\t{1}'.format(self.name, '\n\t'.join(map(repr, self.columns)))

class ColumnDefinition(object):
    def __init__(self, name, ctype=None, pk=False):
        self.name = name
        self.type = ctype
        self.pk = pk
    def __repr__(self):
        extra_info = ''
        if self.pk:
            extra_info += ' - primary key'
        return '{0}: {1}{2}'.format(self.name, self.type, extra_info)

class ForeignKeyConstraintDefinition(object):
    def __init__(self, own_columns, ref_table_name, ref_columns, deferrable=False, on_delete=None):
        self.own_columns = own_columns
        self.ref_table_name = ref_table_name
        self.ref_columns = ref_columns
        self.deferrable = deferrable
        self.on_delete = on_delete
    def __repr__(self):
        extra = ''
        if self.deferrable:
            extra = ' - deferrable'
        if self.on_delete:
            extra += ' - on delete {0}'.format(self.on_delete)
        return 'foreign key ({0}) -> {1} ({2}){3}'.format(', '.join(self.own_columns), self.ref_table_name, ', '.join(self.ref_columns, extra))

class UniqueConstraintDefinition(object):
    def __init__(self, columns):
        self.columns = columns
    def __repr__(self):
        return 'unique: {0}'.format(', '.join(self.columns))

class TableNotFoundError(Exception):
    def __init__(self, table_name):
        Exception.__init__(self, 'table {0} not found in schema'.format(table_name))

class ExecutionError(Exception):
    def __init__(self, message):
        Exception.__init__(self, message)

def quote_name(name):
    return '"{0}"'.format(name)

