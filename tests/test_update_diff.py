from src.update_diff import detect_update_points


def test_detect_multiple_update_points():
    old_data = {
        "table_name": "dwd.a",
        "description": "旧描述",
        "fields": [{"name": "did", "description": "设备", "usage": "标识"}],
        "sql_examples": [],
        "usage_instructions": "",
        "notes": "",
        "data_quality": {},
        "related_cases": [],
    }
    new_data = {
        "table_name": "dwd.a",
        "description": "新描述",
        "fields": [
            {"name": "did", "description": "设备", "usage": "标识"},
            {"name": "uid", "description": "用户", "usage": "关联"},
        ],
        "sql_examples": [{"name": "示例1", "sql": "select 1"}],
        "usage_instructions": "新增说明",
        "notes": "",
        "data_quality": {"coverage": "全量"},
        "related_cases": [{"name": "case1", "type": "SQL", "scenario": "分析"}],
    }
    points = detect_update_points(old_data, new_data)
    assert points == [
        "新增SQL示例",
        "补充字段说明",
        "更新数据质量信息",
        "新增关联案例",
        "完善使用信息",
        "修正描述信息",
    ]
