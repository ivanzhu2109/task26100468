"""按批次持久确认进度的 SQLite -> CSV 快照导出器。

状态目录内维护一个 state.db（SQLite），记录每个任务已确认的最后一行
id、已确认的目标文件字节偏移、已确认行数与完成标记。每批数据写入
目标文件并 fsync 后才在 state.db 中提交进度；若中途崩溃，下次 step
会把目标文件截断回已确认偏移，因此不会产生重复行或重复表头。
"""

from __future__ import annotations

import csv
import dataclasses
import io
import os
import sqlite3
import uuid
from pathlib import Path

_STATE_SCHEMA = """
CREATE TABLE IF NOT EXISTS jobs (
    job_id TEXT PRIMARY KEY,
    source_path TEXT NOT NULL,
    target_path TEXT NOT NULL,
    last_id INTEGER,
    file_offset INTEGER NOT NULL DEFAULT 0,
    rows INTEGER NOT NULL DEFAULT 0,
    done INTEGER NOT NULL DEFAULT 0
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_jobs_paths
    ON jobs (source_path, target_path);
"""


@dataclasses.dataclass(frozen=True)
class JobStatus:
    """任务的持久化状态快照。"""

    job_id: str
    rows: int
    done: bool


def _normalize(path: str | Path) -> str:
    return os.path.normpath(os.path.abspath(os.fspath(path)))


class Exporter:
    """绑定状态目录的导出器。所有调用须串行进行。"""

    def __init__(self, state_dir: str | Path, batch_size: int = 1000) -> None:
        if batch_size < 1:
            raise ValueError("batch_size 必须为正整数")
        self._state_dir = Path(state_dir)
        self._state_dir.mkdir(parents=True, exist_ok=True)
        self._batch_size = batch_size
        self._db = sqlite3.connect(self._state_dir / "state.db")
        self._db.executescript(_STATE_SCHEMA)
        self._db.commit()

    def close(self) -> None:
        self._db.close()

    def __enter__(self) -> "Exporter":
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def create(self, source_path: str | Path, target_path: str | Path) -> str:
        """创建（或复用）导出任务，返回 job_id。

        相同 (source_path, target_path) 对重复调用返回同一 job_id，
        包括已完成的任务。
        """
        source = _normalize(source_path)
        target = _normalize(target_path)
        row = self._db.execute(
            "SELECT job_id FROM jobs WHERE source_path = ? AND target_path = ?",
            (source, target),
        ).fetchone()
        if row is not None:
            return row[0]
        Path(target).parent.mkdir(parents=True, exist_ok=True)
        job_id = uuid.uuid4().hex
        self._db.execute(
            "INSERT INTO jobs (job_id, source_path, target_path) VALUES (?, ?, ?)",
            (job_id, source, target),
        )
        self._db.commit()
        return job_id

    def step(self, job_id: str) -> None:
        """处理下一批数据；无剩余数据时完成导出。已完成任务为无操作。"""
        job = self._load_job(job_id)
        if job["done"]:
            return

        source = sqlite3.connect(f"file:{job['source_path']}?mode=ro", uri=True)
        try:
            if job["last_id"] is None:
                cursor = source.execute(
                    "SELECT id, payload FROM records ORDER BY id ASC LIMIT ?",
                    (self._batch_size,),
                )
            else:
                cursor = source.execute(
                    "SELECT id, payload FROM records WHERE id > ? "
                    "ORDER BY id ASC LIMIT ?",
                    (job["last_id"], self._batch_size),
                )
            batch = cursor.fetchall()
        finally:
            source.close()

        raw = open(job["target_path"], "r+b" if os.path.exists(job["target_path"]) else "w+b")
        try:
            raw.truncate(job["file_offset"])
            raw.seek(job["file_offset"])
            text = io.TextIOWrapper(raw, encoding="utf-8", newline="")
            writer = csv.writer(text)
            if job["file_offset"] == 0:
                writer.writerow(["id", "payload"])
            for row_id, payload in batch:
                writer.writerow([row_id, payload])
            text.flush()
            raw.flush()
            os.fsync(raw.fileno())
            new_offset = raw.tell()
            text.detach()
        finally:
            raw.close()

        if not batch:
            self._db.execute(
                "UPDATE jobs SET done = 1, file_offset = ? WHERE job_id = ?",
                (new_offset, job_id),
            )
        else:
            self._db.execute(
                "UPDATE jobs SET last_id = ?, file_offset = ?, rows = rows + ? "
                "WHERE job_id = ?",
                (batch[-1][0], new_offset, len(batch), job_id),
            )
        self._db.commit()

    def status(self, job_id: str) -> JobStatus:
        """查询已持久确认的行数与完成标记。"""
        row = self._db.execute(
            "SELECT rows, done FROM jobs WHERE job_id = ?", (job_id,)
        ).fetchone()
        if row is None:
            raise KeyError(f"未知任务: {job_id}")
        return JobStatus(job_id=job_id, rows=row[0], done=bool(row[1]))

    def _load_job(self, job_id: str) -> dict:
        row = self._db.execute(
            "SELECT source_path, target_path, last_id, file_offset, done "
            "FROM jobs WHERE job_id = ?",
            (job_id,),
        ).fetchone()
        if row is None:
            raise KeyError(f"未知任务: {job_id}")
        return {
            "source_path": row[0],
            "target_path": row[1],
            "last_id": row[2],
            "file_offset": row[3],
            "done": bool(row[4]),
        }
