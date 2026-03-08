from __future__ import annotations

from pathlib import Path
from typing import List

from config.settings import settings
from src.data_source_manager import DataSourceManager
from src.parser import FileParser
from src.progress_manager import ProgressManager
from src.table_usage_tracker import TableUsageTracker


KNOWN_DOMAINS = ["广告", "应用", "音乐", "公共", "全域搜索", "电商"]


class DataSourceParser:
    def __init__(
        self,
        file_parser: FileParser | None = None,
        data_source_manager: DataSourceManager | None = None,
        progress_manager: ProgressManager | None = None,
        usage_tracker: TableUsageTracker | None = None,
        source_dir: Path | None = None,
    ) -> None:
        self.source_dir = source_dir or settings.source_dir
        self.file_parser = file_parser or FileParser()
        self.data_source_manager = data_source_manager or DataSourceManager()
        self.progress_manager = progress_manager or ProgressManager()
        self.usage_tracker = usage_tracker or TableUsageTracker(
            self.data_source_manager.output_dir / "案例表使用索引.json"
        )

    def prepare_domain_dirs(self) -> None:
        for domain in KNOWN_DOMAINS:
            (self.data_source_manager.output_dir / domain).mkdir(parents=True, exist_ok=True)

    def scan_source_files(self) -> List[Path]:
        files: List[Path] = []
        for ext in ("*.md", "*.sql"):
            files.extend(self.source_dir.rglob(ext))
        return sorted(files)

    def process_file(self, source_file_path: Path) -> None:
        content = source_file_path.read_text(encoding="utf-8")
        parse_result = self.file_parser.parse_file(content, source_file_path)

        business_domain = parse_result.get("business_domain", "其他")
        data_sources = parse_result.get("data_sources", [])
        table_names = set(parse_result.get("table_names", []))

        for ds in data_sources:
            ds["business_domain"] = business_domain
            table_name = ds["table_name"]
            table_names.add(table_name)

            if self.data_source_manager.exists(table_name, business_domain):
                ds_file_path, update_points = self.data_source_manager.update_data_source(
                    table_name, business_domain, ds
                )
                operation_type = "更新数据源"
            else:
                ds_file_path = self.data_source_manager.create_data_source(ds, business_domain)
                update_points = []
                operation_type = "新建数据源"

            self.progress_manager.add_data_source_index(table_name, business_domain, ds_file_path)
            self.progress_manager.add_parse_record(table_name, operation_type, update_points)

        self.usage_tracker.update(source_file_path.as_posix(), sorted(table_names))
        self.progress_manager.mark_file_processed(source_file_path)

    def run(self) -> None:
        self.prepare_domain_dirs()
        all_files = self.scan_source_files()
        self.progress_manager.add_pending_files(all_files)
        for source_file_path in self.progress_manager.get_pending_files():
            if source_file_path.exists():
                self.process_file(source_file_path)


if __name__ == "__main__":
    DataSourceParser().run()
