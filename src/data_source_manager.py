from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Tuple
import re

from config.settings import settings
from src.token_budget import TokenBudget
from src.update_diff import detect_update_points


SECTION_ORDER = [
    "## 1.数据源基本信息",
    "## 2.数据表结构",
    "## 3.SQL使用示例",
    "## 4.使用说明和注意事项",
    "## 5.数据质量说明",
    "## 6.关联案例",
]


class DataSourceManager:
    def __init__(self) -> None:
        self.output_dir = settings.output_dir
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.token_budget = TokenBudget(max_tokens=settings.max_context_tokens)

    def _get_data_source_path(self, table_name: str, business_domain: str) -> Path:
        domain_dir = self.output_dir / business_domain
        domain_dir.mkdir(parents=True, exist_ok=True)
        return domain_dir / f"{table_name}.md"

    def exists(self, table_name: str, business_domain: str) -> bool:
        return self._get_data_source_path(table_name, business_domain).exists()

    def create_data_source(self, data: Dict[str, Any], business_domain: str) -> Path:
        file_path = self._get_data_source_path(data["table_name"], business_domain)
        file_path.write_text(self._generate_markdown(data), encoding="utf-8")
        return file_path

    def update_data_source(
        self, table_name: str, business_domain: str, new_data: Dict[str, Any]
    ) -> Tuple[Path, List[str]]:
        file_path = self._get_data_source_path(table_name, business_domain)
        old_content = file_path.read_text(encoding="utf-8")
        merged_content, update_points = self.merge_data_source(old_content, new_data)
        file_path.write_text(merged_content, encoding="utf-8")
        return file_path, update_points

    def merge_data_source(self, old_content: str, new_data: Dict[str, Any]) -> Tuple[str, List[str]]:
        new_content = self._generate_markdown(new_data)

        old_title = self._extract_title(old_content) or f"# {new_data['table_name']}"
        new_title = self._extract_title(new_content) or old_title
        title = new_title if new_title.strip() else old_title

        old_sections = self._split_sections(old_content)
        new_sections = self._split_sections(new_content)

        merged_blocks: List[str] = [title]
        for section in SECTION_ORDER:
            old_block = old_sections.get(section, "")
            new_block = new_sections.get(section, "")
            if not new_block and old_block:
                merged_block = old_block
            elif not old_block and new_block:
                merged_block = new_block
            elif old_block == new_block:
                merged_block = old_block
            else:
                merged_block = self._merge_section_with_budget(section, old_block, new_block)
            if merged_block.strip():
                merged_blocks.append(merged_block.strip())

        merged_content = "\n\n".join(merged_blocks).strip() + "\n"
        old_struct = self._extract_structured_data(old_content, new_data.get("table_name", ""))
        update_points = detect_update_points(old_struct, new_data)
        return merged_content, update_points

    def _merge_section_with_budget(self, section_name: str, old_block: str, new_block: str) -> str:
        if self.token_budget.can_fit([old_block, new_block]):
            return new_block

        if section_name == "## 3.SQL使用示例":
            return self._merge_sql_examples_incrementally(old_block, new_block)

        # Fallback: for non-SQL sections keep old and append new delta lines.
        old_lines = set(old_block.splitlines())
        merged_lines = old_block.splitlines()
        for line in new_block.splitlines():
            if line not in old_lines:
                merged_lines.append(line)
        return "\n".join(merged_lines).strip()

    def _merge_sql_examples_incrementally(self, old_block: str, new_block: str) -> str:
        merged = old_block.strip() or "## 3.SQL使用示例"
        sql_blocks = re.findall(r"```sql\s*([\s\S]*?)```", new_block)
        for sql in sql_blocks:
            candidate = f"{merged}\n\n### 3.增量示例\n```sql\n{sql.strip()}\n```"
            if self.token_budget.can_fit([candidate]):
                if sql.strip() not in merged:
                    merged = candidate
        return merged.strip()

    @staticmethod
    def _extract_title(content: str) -> str:
        for line in content.splitlines():
            if line.startswith("# "):
                return line.strip()
        return ""

    @staticmethod
    def _split_sections(content: str) -> Dict[str, str]:
        sections: Dict[str, List[str]] = {}
        current: str | None = None
        for line in content.splitlines():
            if line.startswith("## "):
                current = line.strip()
                sections[current] = [line.strip()]
                continue
            if current is not None:
                sections[current].append(line)
        return {key: "\n".join(value).strip() for key, value in sections.items()}

    @staticmethod
    def _extract_structured_data(content: str, fallback_table_name: str) -> Dict[str, Any]:
        def extract_after(marker: str, stop_markers: List[str]) -> str:
            if marker not in content:
                return ""
            start = content.index(marker) + len(marker)
            tail = content[start:]
            stop = len(tail)
            for stop_marker in stop_markers:
                idx = tail.find(stop_marker)
                if idx != -1:
                    stop = min(stop, idx)
            return tail[:stop].strip()

        table_name = fallback_table_name
        title = re.search(r"^#\s+(.+)$", content, re.MULTILINE)
        if title:
            table_name = title.group(1).strip()

        description = extract_after(
            "### 1.2.数据源描述",
            ["### 1.3.业务域", "## 2.数据表结构", "## 3.SQL使用示例"],
        )
        usage_instructions = extract_after("### 4.1.使用说明", ["### 4.2.注意事项", "## 5.数据质量说明"])
        notes = extract_after("### 4.2.注意事项", ["## 5.数据质量说明", "## 6.关联案例"])
        coverage = extract_after("### 5.2.数据覆盖情况", ["### 5.3.上报及时性", "## 6.关联案例"])
        timeliness = extract_after("### 5.3.上报及时性", ["## 6.关联案例"])

        fields: List[Dict[str, str]] = []
        for line in content.splitlines():
            if not line.startswith("|") or line.startswith("| 字段名") or line.startswith("|----------"):
                continue
            parts = [part.strip() for part in line.strip("|").split("|")]
            if len(parts) == 3 and parts[0] and parts[0] != "案例名称":
                fields.append({"name": parts[0], "description": parts[1], "usage": parts[2]})

        sql_examples = [{"name": "已有示例", "sql": sql.strip()} for sql in re.findall(r"```sql\s*([\s\S]*?)```", content)]

        related_cases: List[Dict[str, str]] = []
        in_case_section = False
        for line in content.splitlines():
            if line.strip() == "## 6.关联案例":
                in_case_section = True
                continue
            if in_case_section and line.startswith("## "):
                break
            if in_case_section and line.startswith("|") and "案例名称" not in line and "------------" not in line:
                parts = [part.strip() for part in line.strip("|").split("|")]
                if len(parts) == 3 and parts[0]:
                    related_cases.append({"name": parts[0], "type": parts[1], "scenario": parts[2]})

        return {
            "table_name": table_name,
            "description": description,
            "fields": fields,
            "sql_examples": sql_examples,
            "usage_instructions": usage_instructions,
            "notes": notes,
            "data_quality": {
                "daily_records": "",
                "daily_users": "",
                "coverage": coverage,
                "timeliness": timeliness,
            },
            "related_cases": related_cases,
        }

    def _generate_markdown(self, data: Dict[str, Any]) -> str:
        table_name = data.get("table_name", "")
        business_domain = data.get("business_domain", "其他")
        description = data.get("description", "")
        usage_instructions = data.get("usage_instructions", "")
        notes = data.get("notes", "")
        quality = data.get("data_quality", {})

        lines: List[str] = [
            f"# {table_name}",
            "",
            "## 1.数据源基本信息",
            "",
            "### 1.1.数据源名称",
            data.get("name", table_name),
            "",
            "### 1.2.数据源描述",
            description,
            "",
            "### 1.3.业务域",
            business_domain,
            "",
            "## 2.数据表结构",
            "",
            "### 2.1.表名",
            table_name,
            "",
            "### 2.2.关键字段",
            "| 字段名|字段描述 | 用途说明|",
            "|----------|----------|----------|",
        ]

        for field in data.get("fields", []):
            lines.append(
                f"|{field.get('name', '')}|{field.get('description', '')}|{field.get('usage', '')}|"
            )

        lines.extend(
            [
                "",
                "## 3.SQL使用示例",
                "",
            ]
        )

        for index, example in enumerate(data.get("sql_examples", []), 1):
            lines.extend(
                [
                    f"### 3.{index}.{example.get('name', f'示例{index}')}",
                    "```sql",
                    example.get("sql", ""),
                    "```",
                    "",
                ]
            )

        lines.extend(
            [
                "## 4.使用说明和注意事项",
                "",
                "### 4.1.使用说明",
                usage_instructions,
                "",
                "### 4.2.注意事项",
                notes,
                "",
                "## 5.数据质量说明",
                "",
                "### 5.1.数据量",
                f"- 日记录数：{quality.get('daily_records', '')}",
                f"- 日覆盖用户数：{quality.get('daily_users', '')}",
                "",
                "### 5.2.数据覆盖情况",
                quality.get("coverage", ""),
                "",
                "### 5.3.上报及时性",
                quality.get("timeliness", ""),
                "",
                "## 6.关联案例",
                "|案例名称|案例类型|使用场景|",
                "|------------|------------|------------|",
            ]
        )

        for case in data.get("related_cases", []):
            lines.append(
                f"|{case.get('name', '')}|{case.get('type', '')}|{case.get('scenario', '')}|"
            )

        return "\n".join(lines).strip() + "\n"
