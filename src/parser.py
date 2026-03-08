from __future__ import annotations

import re
from pathlib import Path
from typing import Dict, Any, List


TABLE_PATTERN = re.compile(r"(?:from|join|into|update)\s+([a-zA-Z0-9_.]+)", re.IGNORECASE)


class FileParser:
    def parse_md(self, content: str, file_name: str = "") -> Dict[str, Any]:
        tables = self._extract_tables(content)
        if not tables:
            inferred = self._extract_md_named_tables(content)
            tables = inferred if inferred else ["unknown.table"]
        data_sources = [
            {
                "table_name": table,
                "description": self._first_non_empty_line(content),
                "fields": [],
                "sql_examples": [],
                "usage_instructions": "",
                "notes": "",
                "data_quality": {},
                "related_cases": [{"name": file_name, "type": "Markdown案例", "scenario": "文档引用"}],
            }
            for table in sorted(set(tables))
        ]
        return {
            "business_domain": self._infer_domain(file_name + "\n" + content),
            "data_sources": data_sources,
            "table_names": sorted(set(tables)),
        }

    def parse_sql(self, content: str, file_name: str) -> Dict[str, Any]:
        tables = self._extract_tables(content)
        data_sources = [
            {
                "table_name": table,
                "description": f"由 {file_name} 提取",
                "fields": [],
                "sql_examples": [{"name": file_name, "sql": content.strip()}],
                "usage_instructions": "",
                "notes": "",
                "data_quality": {},
                "related_cases": [{"name": file_name, "type": "SQL案例", "scenario": "SQL引用"}],
            }
            for table in sorted(set(tables))
        ]
        return {
            "business_domain": self._infer_domain(file_name + "\n" + content),
            "data_sources": data_sources,
            "table_names": sorted(set(tables)),
        }

    def parse_file(self, content: str, file_path: Path) -> Dict[str, Any]:
        if file_path.suffix.lower() == ".sql":
            return self.parse_sql(content, file_path.name)
        return self.parse_md(content, file_path.name)

    @staticmethod
    def _extract_tables(text: str) -> List[str]:
        return [m.group(1) for m in TABLE_PATTERN.finditer(text)]

    @staticmethod
    def _extract_md_named_tables(text: str) -> List[str]:
        return re.findall(r"([a-zA-Z][a-zA-Z0-9_]*\.[a-zA-Z][a-zA-Z0-9_]*)", text)

    @staticmethod
    def _first_non_empty_line(text: str) -> str:
        for line in text.splitlines():
            if line.strip():
                return line.strip()
        return ""

    @staticmethod
    def _infer_domain(text: str) -> str:
        mapping = {
            "广告": "广告",
            "ad": "广告",
            "电商": "电商",
            "ecommerce": "电商",
            "music": "音乐",
            "应用": "应用",
            "search": "全域搜索",
        }
        lower = text.lower()
        for key, domain in mapping.items():
            if key in lower:
                return domain
        return "其他"
