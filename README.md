# task26100468 — SQLite → CSV 快照导出库

纯标准库实现的可断点续传 SQLite `records` 表 → CSV 导出库。

## 功能

- 按 `id` 升序导出 `records(id INTEGER PRIMARY KEY, payload TEXT)`，首行为字段名，`NULL` 输出为空字段。
- 任务与进度持久化在状态目录（SQLite 状态库）中；每批提交后持久确认进度。
- 重新打开状态目录可继续导出，最终产物无重复行、无遗漏、无重复表头。
- 相同 `(source_path, target_path)` 重复 `create` 返回同一 `job_id`（含已完成任务）。
- 已完成任务再调 `step` 为无操作；空源也会生成只有表头的完整文件。

## 使用

```python
from csv_snapshot import SnapshotExporter

exp = SnapshotExporter("state_dir", batch_size=1000)
job_id = exp.create("source.db", "out/snapshot.csv")

while not exp.status(job_id)["done"]:
    exp.step(job_id)

print(exp.status(job_id))  # {'confirmed_rows': N, 'done': True}
exp.close()
```

`SnapshotExporter` 也支持 `with` 语句。导出期间数据写入
`<target>.part`，完成后原子重命名为目标文件。

## 运行测试

```sh
python3 -m unittest discover -s tests -v
```
