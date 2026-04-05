from .bplustree import BPlusTree


class Table:
    """Simple table abstraction backed by a B+ Tree index."""

    def __init__(self, name: str, order: int = 4):
        self.name = name
        self.index = BPlusTree(order=order)

    def insert(self, key, record):
        self.index.insert(key, record)

    def search(self, key):
        return self.index.search(key)

    def update(self, key, record):
        return self.index.update(key, record)

    def delete(self, key):
        return self.index.delete(key)

    def range_query(self, start_key, end_key):
        return self.index.range_query(start_key, end_key)

    def get_all(self):
        return self.index.get_all()

    def aggregate(self, operation: str, field: str | None = None, start_key=None, end_key=None):
        return self.index.aggregate(operation, field=field, start_key=start_key, end_key=end_key)
