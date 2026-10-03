"""snapshot_export 的自动化测试。"""

import csv
import os
import sqlite3
import tempfile
import unittest
from pathlib import Path

from snapshot_export import Exporter


def make_source(path, rows):
    db = sqlite3.connect(path)
    db.execute("CREATE TABLE records (id INTEGER PRIMARY KEY, payload TEXT)")
    db.executemany("INSERT INTO records (id, payload) VALUES (?, ?)", rows)
    db.commit()
    db.close()


def read_csv_rows(path):
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.reader(f))


class ExportTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.state_dir = self.root / "state"
        self.source_path = self.root / "source.db"
        self.target_path = self.root / "out" / "export.csv"

    def open_exporter(self, batch_size=3):
        exporter = Exporter(self.state_dir, batch_size=batch_size)
        self.addCleanup(exporter.close)
        return exporter

    def test_resume_across_reopen_with_tricky_rows(self):
        rows = [
            (-(2**63), "负数最小值"),
            (-100, '含,逗号 "引号" 混合'),
            (-1, "跨行\n字段\n三行"),
            (0, None),
            (7, "多字节字符：你好，世界 🌍 café"),
            (10**12, "稀疏id"),
            (2**63 - 1, "末尾,带逗号,"),
        ]
        make_source(self.source_path, rows)

        exporter = self.open_exporter(batch_size=3)
        job_id = exporter.create(self.source_path, self.target_path)
        # 相同路径对重复创建得到同一任务
        self.assertEqual(job_id, exporter.create(self.source_path, self.target_path))

        exporter.step(job_id)  # 第 1 批：3 行
        self.assertEqual(exporter.status(job_id).rows, 3)
        self.assertFalse(exporter.status(job_id).done)
        exporter.step(job_id)  # 第 2 批：3 行
        self.assertEqual(exporter.status(job_id).rows, 6)
        exporter.close()

        # 重新打开状态目录后继续导出
        exporter2 = self.open_exporter(batch_size=3)
        self.assertEqual(exporter2.status(job_id).rows, 6)
        exporter2.step(job_id)  # 第 3 批：1 行
        self.assertFalse(exporter2.status(job_id).done)
        exporter2.step(job_id)  # 无剩余数据 -> 完成
        status = exporter2.status(job_id)
        self.assertTrue(status.done)
        self.assertEqual(status.rows, len(rows))

        parsed = read_csv_rows(self.target_path)
        self.assertEqual(parsed[0], ["id", "payload"])
        self.assertEqual(len(parsed) - 1, len(rows))
        for actual, (row_id, payload) in zip(parsed[1:], rows):
            self.assertEqual(actual[0], str(row_id))
            self.assertEqual(actual[1], "" if payload is None else payload)
        # 表头只出现一次
        self.assertEqual(parsed.count(["id", "payload"]), 1)

    def test_empty_source_and_idempotent_done(self):
        make_source(self.source_path, [])

        exporter = self.open_exporter(batch_size=2)
        job_id = exporter.create(self.source_path, self.target_path)
        exporter.step(job_id)
        status = exporter.status(job_id)
        self.assertTrue(status.done)
        self.assertEqual(status.rows, 0)

        content = self.target_path.read_bytes()
        self.assertEqual(read_csv_rows(self.target_path), [["id", "payload"]])

        # 已完成任务反复 step / create 不改变产物
        exporter.step(job_id)
        exporter.step(job_id)
        self.assertEqual(job_id, exporter.create(self.source_path, self.target_path))
        self.assertEqual(self.target_path.read_bytes(), content)

        # 重新打开后依旧不变
        exporter.close()
        exporter2 = self.open_exporter(batch_size=2)
        exporter2.step(job_id)
        self.assertTrue(exporter2.status(job_id).done)
        self.assertEqual(self.target_path.read_bytes(), content)

    def test_unknown_job_raises(self):
        exporter = self.open_exporter()
        with self.assertRaises(KeyError):
            exporter.status("no-such-job")
        with self.assertRaises(KeyError):
            exporter.step("no-such-job")


if __name__ == "__main__":
    unittest.main()
