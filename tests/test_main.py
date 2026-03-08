from pathlib import Path

from src.main import DataSourceParser


class _FakeFileParser:
    def parse_file(self, content: str, file_path: Path):
        return {
            "business_domain": "电商",
            "data_sources": [
                {
                    "table_name": "dwd.a",
                    "description": "desc",
                    "fields": [],
                    "sql_examples": [{"name": "x", "sql": "select 1"}],
                    "related_cases": [],
                }
            ],
            "table_names": ["dwd.a"],
        }


class _FakeDataSourceManager:
    def exists(self, table_name: str, business_domain: str) -> bool:
        return False

    def create_data_source(self, data, business_domain: str) -> Path:
        return Path("数据源/电商/dwd.a.md")

    def update_data_source(self, table_name: str, business_domain: str, data):
        return Path("数据源/电商/dwd.a.md"), ["新增SQL示例"]


class _FakeProgressManager:
    def __init__(self):
        self.marked = None

    def add_data_source_index(self, table_name: str, business_domain: str, file_path: Path):
        return None

    def add_parse_record(self, table_name: str, operation_type: str, update_points=None):
        return None

    def mark_file_processed(self, source_path: Path):
        self.marked = source_path


class _FakeUsageTracker:
    def __init__(self):
        self.latest = set()

    def update(self, source_file: str, table_names):
        self.latest = set(table_names)


def test_mark_processed_uses_source_file_path(tmp_path: Path):
    source_file = tmp_path / "案例" / "电商" / "a.sql"
    source_file.parent.mkdir(parents=True, exist_ok=True)
    source_file.write_text("select 1", encoding="utf-8")

    fake_progress = _FakeProgressManager()
    parser = DataSourceParser(
        file_parser=_FakeFileParser(),
        data_source_manager=_FakeDataSourceManager(),
        progress_manager=fake_progress,
        usage_tracker=_FakeUsageTracker(),
    )
    parser.process_file(source_file)
    assert fake_progress.marked == source_file
