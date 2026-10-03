"""SQLite records 表到 CSV 的快照导出库（仅标准库）。

状态目录中保存一个 SQLite 状态库，记录每个导出任务的进度
（已确认的最后一个 id 与临时文件偏移量）。导出先写入
``<target>.part`` 临时文件，每批提交后持久确认进度；全部完成
后原子地重命名为目标文件。崩溃或重新打开状态目录后可从上次
确认的进度继续，不产生重复行或重复表头。
"""

from __future__ import annotations

import csv
import hashlib
import io
import os
import sqlite3

_STATE_SCHEMA = """
CREATE TABLE IF NOT EXISTS jobs (
    job_id      TEXT PRIMARY KEY,
    source_path TEXT NOT NULL,
    target_path TEXT NOT NULL,
    done        INTEGER NOT NULL DEFAULT 0,
    last_id     INTEGER,
    offset      INTEGER NOT NULL DEFAULT 0,
    rows        INTEGER NOT NULL DEFAULT 0
)
"""

_HEADER = ("id", "payload")


class SnapshotExporter:
    """基于状态目录的断点续传导出器。

    所有调用假定串行进行。同一状态目录可被重新打开以继续任务。
    """

    def __init__(self, state_dir: str, batch_size: int = 1000) -> None:
        if batch_size < 1:
            raise ValueError("batch_size must be >= 1")
        self._state_dir = os.path.abspath(state_dir)
        self._batch_size = batch_size
        os.makedirs(self._state_dir, exist_ok=True)
        self._db = sqlite3.connect(os.path.join(self._state_dir, "state.db"))
        self._db.execute("PRAGMA journal_mode=WAL")
        self._db.execute("PRAGMA synchronous=FULL")
        self._db.executescript(_STATE_SCHEMA)
        self._db.commit()

    def close(self) -> None:
        self._db.close()

    def __enter__(self) -> "SnapshotExporter":
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    @staticmethod
    def _job_id(source_path: str, target_path: str) -> str:
        key = os.path.abspath(source_path) + "\0" + os.path.abspath(target_path)
        return hashlib.sha256(key.encode("utf-8")).hexdigest()

    def create(self, source_path: str, target_path: str) -> str:
        """创建（或复用）导出任务，返回 job_id。

        相同路径对重复调用返回同一 job_id，包括已完成的任务。
        """
        job_id = self._job_id(source_path, target_path)
        self._db.execute(
            "INSERT OR IGNORE INTO jobs (job_id, source_path, target_path)"
            " VALUES (?, ?, ?)",
            (job_id, os.path.abspath(source_path), os.path.abspath(target_path)),
        )
        self._db.commit()
        return job_id

    def _get_job(self, job_id: str) -> sqlite3.Row:
        row = self._db.execute(
            "SELECT source_path, target_path, done, last_id, offset, rows"
            " FROM jobs WHERE job_id = ?",
            (job_id,),
        ).fetchone()
        if row is None:
            raise KeyError(f"unknown job_id: {job_id}")
        return row

    def step(self, job_id: str) -> None:
        """处理下一批数据；没有剩余数据时完成导出。

        已完成任务再次调用为无操作，不改变产物。
        """
        source_path, target_path, done, last_id, offset, rows = self._get_job(job_id)
        if done:
            return

        part_path = target_path + ".part"
        os.makedirs(os.path.dirname(os.path.abspath(part_path)), exist_ok=True)

        src = sqlite3.connect(f"file:{source_path}?mode=ro", uri=True)
        try:
            if last_id is None:
                cur = src.execute(
                    "SELECT id, payload FROM records ORDER BY id LIMIT ?",
                    (self._batch_size,),
                )
            else:
                cur = src.execute(
                    "SELECT id, payload FROM records WHERE id > ?"
                    " ORDER BY id LIMIT ?",
                    (last_id, self._batch_size),
                )
            batch = cur.fetchall()
        finally:
            src.close()

        if not batch:
            # 无剩余数据：原子地完成导出。
            if not os.path.exists(part_path):
                # 尚未写过任何批次（例如空源的第一步）：先写表头。
                with open(part_path, "w", encoding="utf-8", newline="") as fh:
                    csv.writer(fh).writerow(_HEADER)
                    fh.flush()
                    os.fsync(fh.fileno())
            os.replace(part_path, target_path)
            self._db.execute(
                "UPDATE jobs SET done = 1 WHERE job_id = ?", (job_id,)
            )
            self._db.commit()
            return

        # 追加一个批次：从已确认的偏移处截断后继续写，保证崩溃安全。
        with open(part_path, "a+b") as fh:
            fh.seek(offset)
            fh.truncate()
            wrapper = io.TextIOWrapper(fh, encoding="utf-8", newline="")
            writer = csv.writer(wrapper)
            if offset == 0:
                writer.writerow(_HEADER)
            for row_id, payload in batch:
                writer.writerow((row_id, "" if payload is None else payload))
            wrapper.flush()
            fh.flush()
            os.fsync(fh.fileno())
            new_offset = fh.tell()
            wrapper.detach()

        self._db.execute(
            "UPDATE jobs SET last_id = ?, offset = ?, rows = rows + ?"
            " WHERE job_id = ?",
            (batch[-1][0], new_offset, len(batch), job_id),
        )
        self._db.commit()

    def status(self, job_id: str) -> dict:
        """返回 {"confirmed_rows": int, "done": bool}（来自持久化状态）。"""
        row = self._db.execute(
            "SELECT done, rows FROM jobs WHERE job_id = ?", (job_id,)
        ).fetchone()
        if row is None:
            raise KeyError(f"unknown job_id: {job_id}")
        return {"confirmed_rows": row[1], "done": bool(row[0])}
