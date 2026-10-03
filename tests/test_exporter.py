import csv
import os
import pathlib
import sqlite3
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from csv_snapshot import SnapshotExporter

ROWS = [
    (-9223372036854775808, "min int64,带逗号"),
    (-42, '引号"与\n换行\n混合'),
    (-1, None),
    (0, "零"),
    (7, "多字节字符：你好世界🌍"),
    (1000, "gap-after"),
    (1000000007, "line1\nline2, \"quoted\""),
    (9223372036854775807, "max int64"),
]


def make_source(path, rows):
    db = sqlite3.connect(path)
    db.execute(
        "CREATE TABLE records (id INTEGER PRIMARY KEY, payload TEXT)"
    )
    db.executemany("INSERT INTO records VALUES (?, ?)", rows)
    db.commit()
    db.close()


def read_csv(path):
    with open(path, encoding="utf-8", newline="") as fh:
        return list(csv.reader(fh))


class ResumeExportTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        root = self.tmp.name
        self.source = os.path.join(root, "source.db")
        self.target = os.path.join(root, "out", "snapshot.csv")
        self.state_dir = os.path.join(root, "state")
        make_source(self.source, ROWS)

    def tearDown(self):
        self.tmp.cleanup()

    def expected_rows(self):
        return [["id", "payload"]] + [
            [str(i), "" if p is None else p] for i, p in sorted(ROWS)
        ]

    def test_resume_across_reopen(self):
        # 第一批导出器：只跑两个批次（batch_size=3，共 8 行 -> 3 批）
        exp1 = SnapshotExporter(self.state_dir, batch_size=3)
        job_id = exp1.create(self.source, self.target)
        # 相同路径对重复创建得到同一任务
        self.assertEqual(job_id, exp1.create(self.source, self.target))
        exp1.step(job_id)
        exp1.step(job_id)
        st = exp1.status(job_id)
        self.assertEqual(st["confirmed_rows"], 6)
        self.assertFalse(st["done"])
        exp1.close()

        # 重新打开状态目录继续
        exp2 = SnapshotExporter(self.state_dir, batch_size=3)
        self.assertEqual(job_id, exp2.create(self.source, self.target))
        self.assertEqual(exp2.status(job_id)["confirmed_rows"], 6)
        exp2.step(job_id)  # 剩余 2 行
        self.assertFalse(exp2.status(job_id)["done"])
        exp2.step(job_id)  # 无剩余数据 -> 完成
        st = exp2.status(job_id)
        self.assertTrue(st["done"])
        self.assertEqual(st["confirmed_rows"], 8)
        exp2.close()

        self.assertEqual(read_csv(self.target), self.expected_rows())
        # 表头只出现一次
        with open(self.target, encoding="utf-8") as fh:
            self.assertEqual(fh.readline().strip(), "id,payload")
        self.assertFalse(os.path.exists(self.target + ".part"))


class EmptySourceTest(unittest.TestCase):
    def test_empty_source_and_idempotent_step(self):
        with tempfile.TemporaryDirectory() as root:
            source = os.path.join(root, "empty.db")
            target = os.path.join(root, "empty.csv")
            state_dir = os.path.join(root, "state")
            make_source(source, [])

            exp = SnapshotExporter(state_dir, batch_size=4)
            job_id = exp.create(source, target)
            exp.step(job_id)
            st = exp.status(job_id)
            self.assertTrue(st["done"])
            self.assertEqual(st["confirmed_rows"], 0)

            self.assertEqual(read_csv(target), [["id", "payload"]])
            content = pathlib.Path(target).read_bytes()

            # 已完成任务反复调用 step 不改变产物
            for _ in range(3):
                exp.step(job_id)
            self.assertEqual(pathlib.Path(target).read_bytes(), content)
            self.assertEqual(read_csv(target), [["id", "payload"]])
            exp.close()


if __name__ == "__main__":
    unittest.main()
