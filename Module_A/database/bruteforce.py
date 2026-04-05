class BruteForceDB:
    """Simple baseline store that uses a Python list and linear scans."""

    def __init__(self):
        self.data = []

    def insert(self, key, value=None):
        self.data.append((key, value))

    def search(self, key):
        for k, v in self.data:
            if k == key:
                return v
        return None

    def delete(self, key):
        for idx, (k, _) in enumerate(self.data):
            if k == key:
                self.data.pop(idx)
                return True
        return False

    def update(self, key, new_value):
        for idx, (k, _) in enumerate(self.data):
            if k == key:
                self.data[idx] = (k, new_value)
                return True
        return False

    def range_query(self, start_key, end_key):
        return [(k, v) for (k, v) in self.data if start_key <= k <= end_key]

    def get_all(self):
        return sorted(self.data, key=lambda pair: pair[0])
