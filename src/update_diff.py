from __future__ import annotations

from typing import Any, Dict, List


ORDERED_POINT_TYPES = [
    "新增SQL示例",
    "补充字段说明",
    "更新数据质量信息",
    "新增关联案例",
    "完善使用信息",
    "修正描述信息",
    "合并重复信息",
]


def detect_update_points(old_data: Dict[str, Any], new_data: Dict[str, Any]) -> List[str]:
    points: List[str] = []

    old_sql = {item.get("sql", "").strip() for item in old_data.get("sql_examples", []) if item.get("sql")}
    new_sql = {item.get("sql", "").strip() for item in new_data.get("sql_examples", []) if item.get("sql")}
    if new_sql - old_sql:
        points.append("新增SQL示例")

    old_fields = {item.get("name", "").strip() for item in old_data.get("fields", []) if item.get("name")}
    new_fields = {item.get("name", "").strip() for item in new_data.get("fields", []) if item.get("name")}
    if new_fields - old_fields:
        points.append("补充字段说明")

    old_quality = old_data.get("data_quality", {}) or {}
    new_quality = new_data.get("data_quality", {}) or {}
    for key in ["daily_records", "daily_users", "coverage", "timeliness"]:
        old_value = (old_quality.get(key) or "").strip()
        new_value = (new_quality.get(key) or "").strip()
        if new_value and new_value != old_value:
            points.append("更新数据质量信息")
            break

    old_cases = {item.get("name", "").strip() for item in old_data.get("related_cases", []) if item.get("name")}
    new_cases = {item.get("name", "").strip() for item in new_data.get("related_cases", []) if item.get("name")}
    if new_cases - old_cases:
        points.append("新增关联案例")

    old_usage = (old_data.get("usage_instructions") or "").strip()
    new_usage = (new_data.get("usage_instructions") or "").strip()
    old_notes = (old_data.get("notes") or "").strip()
    new_notes = (new_data.get("notes") or "").strip()
    if (new_usage and new_usage != old_usage) or (new_notes and new_notes != old_notes):
        points.append("完善使用信息")

    old_desc = (old_data.get("description") or "").strip()
    new_desc = (new_data.get("description") or "").strip()
    if new_desc and new_desc != old_desc:
        points.append("修正描述信息")

    if not points:
        points.append("合并重复信息")

    # enforce stable ordering and uniqueness
    uniq = set(points)
    return [point for point in ORDERED_POINT_TYPES if point in uniq]
