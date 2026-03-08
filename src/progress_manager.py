from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import List

from config.settings import settings


PENDING_HEADER = "### 案例文件（.sql 或者 .md）"
PROCESSED_HEADER = "## 已解析文件"
INDEX_HEADER = "## 数据源索引"
RECORD_HEADER = "## 解析记录"


class ProgressManager:
    def __init__(self, output_dir: Path | None = None) -> None:
        self.output_dir = output_dir or settings.output_dir
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.progress_file = self.output_dir / "解析进度.md"
        self._initialize()

    def _initialize(self) -> None:
        if self.progress_file.exists():
            return
        content = """# 数据源解析进度

## 待解析文件

### 案例文件（.sql 或者 .md）

## 已解析文件

## 数据源索引
|数据源表名|业务域|文件路径|
|-----------|-----------|-----------|

## 解析记录
|数据源名称|解析时间|操作类型|更新内容|
|-----------|-----------|-----------|-----------|

## 案例表使用索引
- 文件：案例表使用索引.json
"""
        self.progress_file.write_text(content, encoding="utf-8")

    def _read_lines(self) -> List[str]:
        return self.progress_file.read_text(encoding="utf-8").splitlines()

    def _write_lines(self, lines: List[str]) -> None:
        self.progress_file.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")

    @staticmethod
    def _section_range(lines: List[str], section_title: str) -> tuple[int, int]:
        start = -1
        end = len(lines)
        for idx, line in enumerate(lines):
            if line.strip() == section_title:
                start = idx + 1
                continue
            if start != -1 and idx > start and line.startswith("## "):
                end = idx
                break
        if start == -1:
            raise ValueError(f"section not found: {section_title}")
        return start, end

    def add_pending_files(self, files: List[Path]) -> None:
        lines = self._read_lines()
        start, end = self._section_range(lines, PENDING_HEADER)
        existing = {
            line[6:].strip()
            for line in lines[start:end]
            if line.startswith("- [ ] ")
        }
        additions: List[str] = []
        for file in files:
            file_path = file.as_posix()
            if file_path not in existing:
                additions.append(f"- [ ] {file_path}")
        if additions:
            lines[start:start] = additions
            self._write_lines(lines)

    def get_pending_files(self) -> List[Path]:
        lines = self._read_lines()
        start, end = self._section_range(lines, PENDING_HEADER)
        pending: List[Path] = []
        for line in lines[start:end]:
            if line.startswith("- [ ] "):
                pending.append(Path(line[6:].strip()))
        return pending

    def mark_file_processed(self, source_file_path: Path) -> None:
        lines = self._read_lines()
        source = source_file_path.as_posix()

        # Remove from pending section.
        lines = [line for line in lines if line != f"- [ ] {source}"]

        # Idempotent insert into processed section.
        processed_start, processed_end = self._section_range(lines, PROCESSED_HEADER)
        existing_processed = {
            line[6:].strip()
            for line in lines[processed_start:processed_end]
            if line.startswith("- [x] ")
        }
        if source not in existing_processed:
            lines.insert(processed_start, f"- [x] {source}")
        self._write_lines(lines)

    def add_data_source_index(self, table_name: str, business_domain: str, file_path: Path) -> None:
        lines = self._read_lines()
        start, end = self._section_range(lines, INDEX_HEADER)

        row_key = f"|{table_name}|"
        for line in lines[start:end]:
            if line.startswith(row_key):
                return

        relative_path = file_path.as_posix()
        lines.insert(start, f"|{table_name}|{business_domain}|{relative_path}|")
        self._write_lines(lines)

    def add_parse_record(self, table_name: str, operation_type: str, update_points: List[str] | None = None) -> None:
        lines = self._read_lines()
        start, _ = self._section_range(lines, RECORD_HEADER)
        timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
        update = "；".join(update_points or [])
        lines.insert(start, f"|{table_name}|{timestamp}|{operation_type}|{update}|")
        self._write_lines(lines)
