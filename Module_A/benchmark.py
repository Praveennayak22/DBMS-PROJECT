import random
import statistics
import time

import matplotlib.pyplot as plt
import pandas as pd

from database.bplustree import BPlusTree
from database.bruteforce import BruteForceDB


class BenchmarkRunner:
    def __init__(self, sizes=None, seed=42):
        self.sizes = sizes or [1000, 5000, 10000, 20000]
        self.seed = seed
        self.rng = random.Random(seed)
        self.insert_delete_trials = 5
        self.query_repeats = 25

    @staticmethod
    def _timeit(func, *args, repeat=1):
        start = time.perf_counter()
        for _ in range(repeat):
            func(*args)
        # Return average time per run to reduce noise while preserving units (seconds).
        return (time.perf_counter() - start) / repeat

    def _median_trial_time(self, func_factory, trials):
        times = []
        for _ in range(trials):
            times.append(self._timeit(func_factory()))
        return statistics.median(times)

    @staticmethod
    def _memory_bytes(obj):
        import sys
        from collections import deque

        seen = set()
        total_size = 0
        queue = deque([obj])

        while queue:
            x = queue.popleft()
            obj_id = id(x)
            if obj_id in seen:
                continue
            seen.add(obj_id)

            total_size += sys.getsizeof(x)

            if isinstance(x, dict):
                for k, v in x.items():
                    queue.append(k)
                    queue.append(v)
            elif isinstance(x, (list, tuple, set)):
                queue.extend(x)
            elif hasattr(x, "__dict__"):
                queue.append(vars(x))

        return total_size

    def run(self):
        rows = []

        for size in self.sizes:
            keys = self.rng.sample(range(size * 20), size)
            query_keys = self.rng.sample(keys, min(200, len(keys)))
            sorted_keys = sorted(keys)
            range_width = max(10, size // 20)  # ~5% of keys, avoids near full-scan range windows.
            start_idx = max(0, (len(sorted_keys) - range_width) // 2)
            end_idx = min(len(sorted_keys) - 1, start_idx + range_width - 1)
            lo = sorted_keys[start_idx]
            hi = sorted_keys[end_idx]

            # Insert timing is measured on fresh stores per trial.
            bt_insert = self._median_trial_time(
                lambda: (lambda: _insert_trial(BPlusTree(order=8), keys)),
                self.insert_delete_trials,
            )
            bf_insert = self._median_trial_time(
                lambda: (lambda: _insert_trial(BruteForceDB(), keys)),
                self.insert_delete_trials,
            )

            # Build canonical populated stores for search/range/memory measurements.
            bt = BPlusTree(order=8)
            bf = BruteForceDB()
            for k in keys:
                bt.insert(k, {"id": k})
                bf.insert(k, {"id": k})

            bt_search = self._timeit(lambda: [bt.search(k) for k in query_keys], repeat=self.query_repeats)
            bf_search = self._timeit(lambda: [bf.search(k) for k in query_keys], repeat=self.query_repeats)

            bt_range = self._timeit(bt.range_query, lo, hi, repeat=self.query_repeats)
            bf_range = self._timeit(bf.range_query, lo, hi, repeat=self.query_repeats)

            delete_keys = query_keys[:100]
            bt_delete = self._median_trial_time(
                lambda: (
                    lambda: _delete_trial(BPlusTree(order=8), keys, delete_keys)
                ),
                self.insert_delete_trials,
            )
            bf_delete = self._median_trial_time(
                lambda: (
                    lambda: _delete_trial(BruteForceDB(), keys, delete_keys)
                ),
                self.insert_delete_trials,
            )

            bt_memory = self._memory_bytes(bt)
            bf_memory = self._memory_bytes(bf)

            rows.extend(
                [
                    {"size": size, "operation": "insert", "store": "bplustree", "time": bt_insert},
                    {"size": size, "operation": "insert", "store": "bruteforce", "time": bf_insert},
                    {"size": size, "operation": "search", "store": "bplustree", "time": bt_search},
                    {"size": size, "operation": "search", "store": "bruteforce", "time": bf_search},
                    {"size": size, "operation": "range", "store": "bplustree", "time": bt_range},
                    {"size": size, "operation": "range", "store": "bruteforce", "time": bf_range},
                    {"size": size, "operation": "delete", "store": "bplustree", "time": bt_delete},
                    {"size": size, "operation": "delete", "store": "bruteforce", "time": bf_delete},
                    {"size": size, "operation": "memory", "store": "bplustree", "time": bt_memory},
                    {"size": size, "operation": "memory", "store": "bruteforce", "time": bf_memory},
                ]
            )

        return pd.DataFrame(rows)


def _insert_trial(store, keys):
    for k in keys:
        store.insert(k, {"id": k})


def _delete_trial(store, keys, delete_keys):
    for k in keys:
        store.insert(k, {"id": k})
    for k in delete_keys:
        store.delete(k)


def plot_results(df: pd.DataFrame, output_path: str = "benchmark_results.png"):
    fig, axes = plt.subplots(2, 2, figsize=(12, 8), constrained_layout=True)
    operation_axes = {
        "insert": axes[0][0],
        "search": axes[0][1],
        "range": axes[1][0],
        "delete": axes[1][1],
    }

    for operation, ax in operation_axes.items():
        subset = df[df["operation"] == operation]
        for store in ["bplustree", "bruteforce"]:
            s = subset[subset["store"] == store]
            ax.plot(s["size"], s["time"], marker="o", label=store)
        ax.set_title(operation.title())
        ax.set_xlabel("Number of keys")
        ax.set_ylabel("Time (seconds)")
        ax.grid(True, alpha=0.3)
        ax.legend()

    fig.savefig(output_path, dpi=200)


if __name__ == "__main__":
    runner = BenchmarkRunner()
    df = runner.run()
    df.to_csv("benchmark_results.csv", index=False)
    plot_results(df)
    print(df.groupby(["operation", "store"])["time"].mean())
