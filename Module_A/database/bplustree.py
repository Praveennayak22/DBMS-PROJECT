from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, List, Optional, Tuple

try:
    from graphviz import Digraph
except Exception:  # pragma: no cover
    Digraph = None

try:
    from graphviz.backend import ExecutableNotFound
except Exception:  # pragma: no cover
    ExecutableNotFound = None


@dataclass
class BPlusTreeNode:
    order: int
    is_leaf: bool = False
    keys: List[Any] = field(default_factory=list)
    values: List[Any] = field(default_factory=list)
    children: List["BPlusTreeNode"] = field(default_factory=list)
    next: Optional["BPlusTreeNode"] = None

    @property
    def max_keys(self) -> int:
        return self.order - 1


class BPlusTree:
    """In-memory B+ Tree supporting exact and range queries."""

    def __init__(self, order: int = 4):
        if order < 3:
            raise ValueError("B+ Tree order must be >= 3")
        self.order = order
        self.root = BPlusTreeNode(order=order, is_leaf=True)

    def _find_leaf(self, key) -> BPlusTreeNode:
        node = self.root
        while not node.is_leaf:
            idx = 0
            while idx < len(node.keys) and key >= node.keys[idx]:
                idx += 1
            node = node.children[idx]
        return node

    def search(self, key):
        leaf = self._find_leaf(key)
        for idx, k in enumerate(leaf.keys):
            if k == key:
                return leaf.values[idx]
        return None

    def insert(self, key, value):
        root = self.root
        if len(root.keys) == root.max_keys:
            new_root = BPlusTreeNode(order=self.order, is_leaf=False, children=[root])
            self._split_child(new_root, 0)
            self.root = new_root
        self._insert_non_full(self.root, key, value)

    def _insert_non_full(self, node: BPlusTreeNode, key, value):
        if node.is_leaf:
            idx = 0
            while idx < len(node.keys) and node.keys[idx] < key:
                idx += 1
            if idx < len(node.keys) and node.keys[idx] == key:
                node.values[idx] = value
                return
            node.keys.insert(idx, key)
            node.values.insert(idx, value)
            return

        idx = 0
        while idx < len(node.keys) and key >= node.keys[idx]:
            idx += 1

        child = node.children[idx]
        if len(child.keys) == child.max_keys:
            self._split_child(node, idx)
            if key >= node.keys[idx]:
                idx += 1
        self._insert_non_full(node.children[idx], key, value)

    def _split_child(self, parent: BPlusTreeNode, index: int):
        child = parent.children[index]
        new_node = BPlusTreeNode(order=self.order, is_leaf=child.is_leaf)
        mid = len(child.keys) // 2

        if child.is_leaf:
            new_node.keys = child.keys[mid:]
            new_node.values = child.values[mid:]
            child.keys = child.keys[:mid]
            child.values = child.values[:mid]
            new_node.next = child.next
            child.next = new_node
            promoted_key = new_node.keys[0]

            parent.keys.insert(index, promoted_key)
            parent.children.insert(index + 1, new_node)
        else:
            promoted_key = child.keys[mid]
            new_node.keys = child.keys[mid + 1 :]
            new_node.children = child.children[mid + 1 :]

            child.keys = child.keys[:mid]
            child.children = child.children[: mid + 1]

            parent.keys.insert(index, promoted_key)
            parent.children.insert(index + 1, new_node)

    def delete(self, key):
        success = self._delete(self.root, key)
        if not self.root.is_leaf and len(self.root.keys) == 0:
            self.root = self.root.children[0]
        return success

    def _delete(self, node: BPlusTreeNode, key):
        if node.is_leaf:
            if key not in node.keys:
                return False
            idx = node.keys.index(key)
            node.keys.pop(idx)
            node.values.pop(idx)
            return True

        idx = 0
        while idx < len(node.keys) and key >= node.keys[idx]:
            idx += 1

        child = node.children[idx]
        deleted = self._delete(child, key)
        if not deleted:
            return False

        min_keys = (self.order - 1) // 2
        if child is self.root:
            return True

        if len(child.keys) < min_keys:
            self._fill_child(node, idx)

        self._refresh_internal_keys(node)
        return True

    def _fill_child(self, node: BPlusTreeNode, index: int):
        child = node.children[index]
        min_keys = (self.order - 1) // 2

        if index > 0 and len(node.children[index - 1].keys) > min_keys:
            self._borrow_from_prev(node, index)
            return

        if index < len(node.children) - 1 and len(node.children[index + 1].keys) > min_keys:
            self._borrow_from_next(node, index)
            return

        if index < len(node.children) - 1:
            self._merge(node, index)
        else:
            self._merge(node, index - 1)

    def _borrow_from_prev(self, node: BPlusTreeNode, index: int):
        child = node.children[index]
        sibling = node.children[index - 1]

        if child.is_leaf:
            child.keys.insert(0, sibling.keys.pop())
            child.values.insert(0, sibling.values.pop())
            node.keys[index - 1] = child.keys[0]
        else:
            child.keys.insert(0, node.keys[index - 1])
            node.keys[index - 1] = sibling.keys.pop()
            child.children.insert(0, sibling.children.pop())

    def _borrow_from_next(self, node: BPlusTreeNode, index: int):
        child = node.children[index]
        sibling = node.children[index + 1]

        if child.is_leaf:
            child.keys.append(sibling.keys.pop(0))
            child.values.append(sibling.values.pop(0))
            node.keys[index] = sibling.keys[0] if sibling.keys else child.keys[-1]
        else:
            child.keys.append(node.keys[index])
            node.keys[index] = sibling.keys.pop(0)
            child.children.append(sibling.children.pop(0))

    def _merge(self, node: BPlusTreeNode, index: int):
        left = node.children[index]
        right = node.children[index + 1]

        if left.is_leaf:
            left.keys.extend(right.keys)
            left.values.extend(right.values)
            left.next = right.next
        else:
            separator = node.keys[index]
            left.keys.append(separator)
            left.keys.extend(right.keys)
            left.children.extend(right.children)

        node.keys.pop(index)
        node.children.pop(index + 1)

    def _refresh_internal_keys(self, node: BPlusTreeNode):
        if node.is_leaf:
            return
        refreshed = []
        for i in range(1, len(node.children)):
            refreshed.append(self._leftmost_key(node.children[i]))
        node.keys = refreshed

    def _leftmost_key(self, node: BPlusTreeNode):
        while not node.is_leaf:
            node = node.children[0]
        return node.keys[0] if node.keys else None

    def update(self, key, new_value):
        leaf = self._find_leaf(key)
        for i, k in enumerate(leaf.keys):
            if k == key:
                leaf.values[i] = new_value
                return True
        return False

    def range_query(self, start_key, end_key):
        if start_key > end_key:
            return []
        leaf = self._find_leaf(start_key)
        result = []

        while leaf is not None:
            for idx, k in enumerate(leaf.keys):
                if k < start_key:
                    continue
                if k > end_key:
                    return result
                result.append((k, leaf.values[idx]))
            leaf = leaf.next
        return result

    def get_all(self):
        node = self.root
        while not node.is_leaf:
            node = node.children[0]

        output = []
        while node is not None:
            output.extend(zip(node.keys, node.values))
            node = node.next
        return output

    def aggregate(self, operation: str, field: Optional[str] = None, start_key=None, end_key=None):
        """Aggregate records over all keys or within an optional key range.

        Supported operations: count, sum, avg, min, max.
        If `field` is provided, values are expected to be dict-like records.
        """
        op = operation.lower()
        if op not in {"count", "sum", "avg", "min", "max"}:
            raise ValueError("Unsupported aggregation operation")

        if start_key is not None and end_key is not None:
            pairs = self.range_query(start_key, end_key)
        else:
            pairs = self.get_all()

        if op == "count":
            return len(pairs)

        if field is None:
            values = [k for k, _ in pairs]
        else:
            values = []
            for _, record in pairs:
                if isinstance(record, dict) and field in record and record[field] is not None:
                    values.append(record[field])

        if not values:
            return None

        if op == "sum":
            return sum(values)
        if op == "avg":
            return sum(values) / len(values)
        if op == "min":
            return min(values)
        return max(values)

    def visualize_tree(self, filename: Optional[str] = None):
        if Digraph is None:
            raise RuntimeError("graphviz package is not installed")

        dot = Digraph(comment="B+ Tree")
        dot.attr(rankdir="TB")

        self._add_nodes(dot, self.root)
        self._add_edges(dot, self.root)

        if filename:
            try:
                dot.render(filename, format="png", cleanup=True)
            except Exception as exc:
                is_missing_dot = (
                    ExecutableNotFound is not None and isinstance(exc, ExecutableNotFound)
                )
                if is_missing_dot:
                    dot_path = f"{filename}.dot"
                    dot.save(dot_path)
                    raise RuntimeError(
                        "Graphviz executable 'dot' was not found in PATH. "
                        f"Saved DOT source to '{dot_path}'. Install Graphviz system binaries "
                        "and add them to PATH to render PNG output."
                    ) from exc
                raise
        return dot

    def _node_id(self, node: BPlusTreeNode) -> str:
        return str(id(node))

    def _add_nodes(self, dot: Any, node: BPlusTreeNode):
        label = "|".join(str(k) for k in node.keys)
        shape = "record" if not node.is_leaf else "box"
        fill = "lightblue" if node.is_leaf else "lightgray"
        dot.node(self._node_id(node), label=label or "empty", shape=shape, style="filled", fillcolor=fill)

        if not node.is_leaf:
            for child in node.children:
                self._add_nodes(dot, child)

    def _add_edges(self, dot: Any, node: BPlusTreeNode):
        if node.is_leaf:
            if node.next is not None:
                dot.edge(self._node_id(node), self._node_id(node.next), style="dashed", color="blue")
            return

        for child in node.children:
            dot.edge(self._node_id(node), self._node_id(child))
            self._add_edges(dot, child)


class PerformanceAnalyzer:
    """Compares B+ Tree and BruteForceDB for standard DB-like operations."""

    def __init__(self, indexed_store, brute_force_store):
        self.indexed_store = indexed_store
        self.brute_force_store = brute_force_store

    @staticmethod
    def memory_usage_bytes(obj) -> int:
        import sys

        seen = set()

        def sizeof(x):
            obj_id = id(x)
            if obj_id in seen:
                return 0
            seen.add(obj_id)
            size = sys.getsizeof(x)
            if isinstance(x, dict):
                size += sum(sizeof(k) + sizeof(v) for k, v in x.items())
            elif isinstance(x, (list, tuple, set)):
                size += sum(sizeof(i) for i in x)
            elif hasattr(x, "__dict__"):
                size += sizeof(vars(x))
            return size

        return sizeof(obj)

    @staticmethod
    def _time_operation(func, *args, repeat: int = 1):
        import time

        start = time.perf_counter()
        for _ in range(repeat):
            func(*args)
        end = time.perf_counter()
        return end - start

    def benchmark(self, keys, query_range):
        import random

        results = {
            "insert": {},
            "search": {},
            "delete": {},
            "range": {},
            "random_mix": {},
            "memory": {},
        }

        for key in keys:
            self.indexed_store.insert(key, {"id": key})
            self.brute_force_store.insert(key, {"id": key})

        sample_keys = random.sample(keys, min(100, len(keys)))
        search_repeat = 25
        range_repeat = 25

        # Search
        indexed_search = self._time_operation(
            lambda: [self.indexed_store.search(k) for k in sample_keys],
            repeat=search_repeat,
        ) / search_repeat
        brute_search = self._time_operation(
            lambda: [self.brute_force_store.search(k) for k in sample_keys],
            repeat=search_repeat,
        ) / search_repeat
        results["search"] = {"bplustree": indexed_search, "bruteforce": brute_search}

        # Range query
        start_key, end_key = query_range
        if keys:
            sorted_keys = sorted(keys)
            width = max(10, len(sorted_keys) // 20)
            start_idx = max(0, (len(sorted_keys) - width) // 2)
            end_idx = min(len(sorted_keys) - 1, start_idx + width - 1)
            start_key = sorted_keys[start_idx]
            end_key = sorted_keys[end_idx]

        indexed_range = self._time_operation(
            self.indexed_store.range_query,
            start_key,
            end_key,
            repeat=range_repeat,
        ) / range_repeat
        brute_range = self._time_operation(
            self.brute_force_store.range_query,
            start_key,
            end_key,
            repeat=range_repeat,
        ) / range_repeat
        results["range"] = {"bplustree": indexed_range, "bruteforce": brute_range}

        # Random mixed operations
        def mixed_ops(store):
            for k in sample_keys[:30]:
                store.search(k)
            for k in sample_keys[30:60]:
                store.update(k, {"id": k, "updated": True})
            for k in sample_keys[60:90]:
                store.delete(k)

        indexed_mix = self._time_operation(mixed_ops, self.indexed_store)
        brute_mix = self._time_operation(mixed_ops, self.brute_force_store)
        results["random_mix"] = {"bplustree": indexed_mix, "bruteforce": brute_mix}

        # Memory usage
        results["memory"] = {
            "bplustree": self.memory_usage_bytes(self.indexed_store),
            "bruteforce": self.memory_usage_bytes(self.brute_force_store),
        }

        return results
