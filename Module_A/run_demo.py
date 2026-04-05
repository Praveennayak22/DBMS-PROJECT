from database.bplustree import BPlusTree


def main():
    tree = BPlusTree(order=4)
    for key in [10, 20, 5, 6, 12, 30, 7, 17]:
        tree.insert(key, {"student_id": key, "name": f"Student-{key}"})

    print("Exact search for 12:", tree.search(12))
    print("Range query [6, 20]:", tree.range_query(6, 20))

    tree.update(12, {"student_id": 12, "name": "Student-12-Updated"})
    print("After update key 12:", tree.search(12))

    tree.delete(7)
    print("After delete key 7 all data:", tree.get_all())
    print("Aggregate count [6,20]:", tree.aggregate("count", start_key=6, end_key=20))
    print("Aggregate max key [6,20]:", tree.aggregate("max", start_key=6, end_key=20))


if __name__ == "__main__":
    main()
