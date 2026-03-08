from pathlib import Path

from src.quality_checker import QualityChecker


def test_report_file_name_matches_requirement(tmp_path: Path):
    checker = QualityChecker(output_dir=tmp_path / "数据源")
    assert checker.report_file.name == "检察报告.md"


def test_detect_duplicates_with_high_similarity_content(tmp_path: Path):
    output_dir = tmp_path / "数据源"
    output_dir.mkdir(parents=True, exist_ok=True)
    domain = output_dir / "电商"
    domain.mkdir(parents=True, exist_ok=True)

    content_a = "# dwd.a\n\n## 1.数据源基本信息\n电商交易明细\n"
    content_b = "# dwd.b\n\n## 1.数据源基本信息\n电商交易明细\n"
    (domain / "a.md").write_text(content_a, encoding="utf-8")
    (domain / "b.md").write_text(content_b, encoding="utf-8")

    checker = QualityChecker(output_dir=output_dir)
    duplicates = checker.detect_duplicates()
    assert len(duplicates) > 0
