from pathlib import Path

from src.table_usage_tracker import TableUsageTracker


def test_usage_tracker_persist_and_load(tmp_path: Path):
    tracker = TableUsageTracker(tmp_path / "usage.json")
    tracker.update("案例/电商/a.sql", ["dwd.a", "dwd.b", "dwd.a"])
    tracker.update("案例/电商/b.md", ["dwd.c"])

    loaded = TableUsageTracker(tmp_path / "usage.json")
    assert loaded.get_tables_for_file("案例/电商/a.sql") == {"dwd.a", "dwd.b"}
    assert loaded.get_all_used_tables() == {"dwd.a", "dwd.b", "dwd.c"}
