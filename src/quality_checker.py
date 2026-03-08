from __future__ import annotations

import difflib
from pathlib import Path
from typing import Dict, List, Tuple

from config.settings import settings
from src.table_usage_tracker import TableUsageTracker


class QualityChecker:
    def __init__(self, output_dir: Path | None = None) -> None:
        self.output_dir = output_dir or settings.output_dir
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.report_file = self.output_dir / "检察报告.md"
        self.usage_tracker = TableUsageTracker(self.output_dir / "案例表使用索引.json")

    def scan_all_data_sources(self) -> Dict[str, List[Path]]:
        grouped: Dict[str, List[Path]] = {}
        for md_file in self.output_dir.rglob("*.md"):
            if md_file.name in {"解析进度.md", "检察报告.md"}:
                continue
            lines = md_file.read_text(encoding="utf-8").splitlines()
            if not lines:
                continue
            first_line = lines[0].strip()
            if not first_line.startswith("# "):
                continue
            table_name = first_line[2:].strip()
            grouped.setdefault(table_name, []).append(md_file)
        return grouped

    def detect_duplicates(self) -> List[Tuple[str, List[Path]]]:
        duplicates: List[Tuple[str, List[Path]]] = []
        grouped = self.scan_all_data_sources()
        for table_name, files in grouped.items():
            if len(files) > 1:
                duplicates.append((table_name, files))

        # Detect high-similarity duplicates even if table names differ.
        table_to_file: List[Tuple[str, Path]] = [
            (table_name, files[0]) for table_name, files in grouped.items() if files
        ]
        seen_pairs = set()
        for i, (table_a, file_a) in enumerate(table_to_file):
            text_a = self._normalized_content(file_a)
            for table_b, file_b in table_to_file[i + 1 :]:
                pair_key = tuple(sorted([table_a, table_b]))
                if pair_key in seen_pairs:
                    continue
                seen_pairs.add(pair_key)
                ratio = difflib.SequenceMatcher(a=text_a, b=self._normalized_content(file_b)).ratio()
                if ratio >= 0.9:
                    duplicates.append((f"相似内容:{table_a}~{table_b}", [file_a, file_b]))
        return duplicates

    @staticmethod
    def _normalized_content(file_path: Path) -> str:
        lines = file_path.read_text(encoding="utf-8").splitlines()
        if lines and lines[0].startswith("# "):
            lines = lines[1:]
        return "\n".join(line.strip() for line in lines if line.strip())

    def detect_missing(self, used_tables: List[str] | None = None) -> List[str]:
        expected = set(used_tables or self.usage_tracker.get_all_used_tables())
        existing = set(self.scan_all_data_sources().keys())
        return sorted(expected - existing)

    def generate_report(
        self, duplicates: List[Tuple[str, List[Path]]], missing: List[str]
    ) -> Path:
        total_tables = len(self.scan_all_data_sources())
        denominator = max(total_tables + len(missing), 1)
        completeness = 100 - (len(missing) / denominator * 100)

        lines: List[str] = [
            "# 数据源检察报告",
            "",
            "## 1.重复数据源检测",
        ]
        if duplicates:
            lines.append(f"发现 {len(duplicates)} 个重复数据源：")
            lines.append("")
            for table_name, files in duplicates:
                lines.append(f"### {table_name}")
                for file in files:
                    lines.append(f"- {file.relative_to(self.output_dir).as_posix()}")
                lines.append("")
        else:
            lines.extend(["未发现重复数据源", ""])

        lines.append("## 2.遗漏数据源检测")
        if missing:
            lines.append(f"发现 {len(missing)} 个遗漏的数据源：")
            lines.append("")
            lines.extend([f"- {table}" for table in missing])
            lines.append("")
        else:
            lines.extend(["未发现遗漏数据源", ""])

        lines.extend(
            [
                "## 3.完整性评估",
                f"- 总数据源数量：{total_tables}",
                f"- 重复数据源数量：{len(duplicates)}",
                f"- 遗漏数据源数量：{len(missing)}",
                f"- 完整性：{completeness:.1f}%",
                "",
            ]
        )
        self.report_file.write_text("\n".join(lines), encoding="utf-8")
        return self.report_file

    def run(self) -> Path:
        duplicates = self.detect_duplicates()
        missing = self.detect_missing()
        return self.generate_report(duplicates, missing)
