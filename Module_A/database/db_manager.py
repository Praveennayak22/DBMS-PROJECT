from .table import Table


class DBManager:
    """Manages multiple named tables for Module A."""

    def __init__(self):
        self.tables = {}

    def create_table(self, table_name: str, order: int = 4):
        if table_name in self.tables:
            raise ValueError(f"Table '{table_name}' already exists")
        self.tables[table_name] = Table(table_name, order=order)
        return self.tables[table_name]

    def get_table(self, table_name: str):
        if table_name not in self.tables:
            raise KeyError(f"Table '{table_name}' not found")
        return self.tables[table_name]

    def drop_table(self, table_name: str):
        if table_name in self.tables:
            del self.tables[table_name]
            return True
        return False

    def list_tables(self):
        return sorted(self.tables.keys())
