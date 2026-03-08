from src.data_source_manager import DataSourceManager


def test_merge_only_changed_sections():
    manager = DataSourceManager()
    old_md = (
        "# t\n\n"
        "## 1.数据源基本信息\n"
        "A\n\n"
        "## 3.SQL使用示例\n"
        "### 3.1.旧示例\n"
        "```sql\nselect 0\n```\n"
    )
    new_data = {
        "table_name": "t",
        "description": "A",
        "sql_examples": [{"name": "x", "sql": "select 1"}],
    }
    merged, points = manager.merge_data_source(old_md, new_data)
    assert "select 1" in merged
    assert "新增SQL示例" in points
