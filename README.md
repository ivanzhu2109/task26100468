# snapshot_export

仅用 Python 标准库实现的 SQLite `records` 表到 CSV 的快照导出库。
按 `id` 升序分批导出，进度持久化在状态目录中，崩溃或重新打开后可断点续导，
产物无重复行、无遗漏、无重复表头。

## 使用

```python
from snapshot_export import Exporter

exporter = Exporter("path/to/state_dir", batch_size=1000)
job_id = exporter.create("source.db", "out/export.csv")  # 相同路径对复用同一任务

while not exporter.status(job_id).done:
    exporter.step(job_id)   # 每批写入并 fsync 后才提交进度

exporter.close()
```

- `create(source_path, target_path)`：创建任务并返回 `job_id`；相同路径对（含已完成任务）返回同一 `job_id`。
- `step(job_id)`：处理下一批；无剩余数据时完成导出；已完成任务为无操作。
- `status(job_id)`：返回 `JobStatus(job_id, rows, done)`，`rows` 为已持久确认的行数。

约定：源库须含 `records(id INTEGER PRIMARY KEY, payload TEXT)` 表；任务存活期间源库不变、
目标路径由任务独占、所有调用串行。`NULL` 输出为空字段，空源生成仅含表头的文件。

## 运行测试

```sh
python3 -m unittest discover -s tests -v
```
