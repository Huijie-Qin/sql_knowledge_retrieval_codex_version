from pathlib import Path

from src.quality_checker import QualityChecker
from src.table_usage_tracker import TableUsageTracker


def test_detect_missing_with_usage_index(tmp_path: Path):
    output_dir = tmp_path / "数据源"
    output_dir.mkdir(parents=True, exist_ok=True)

    domain_dir = output_dir / "电商"
    domain_dir.mkdir(parents=True, exist_ok=True)
    (domain_dir / "dwd.a.md").write_text("# dwd.a\n", encoding="utf-8")

    tracker = TableUsageTracker(output_dir / "案例表使用索引.json")
    tracker.update("案例/电商/a.sql", ["dwd.a", "dwd.b"])

    checker = QualityChecker(output_dir=output_dir)
    missing = checker.detect_missing()
    assert missing == ["dwd.b"]
