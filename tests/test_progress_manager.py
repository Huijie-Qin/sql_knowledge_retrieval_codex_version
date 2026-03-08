from pathlib import Path

from src.progress_manager import ProgressManager


def test_get_pending_files_can_parse_section(tmp_path: Path):
    manager = ProgressManager(output_dir=tmp_path / "数据源")
    manager.add_pending_files([Path("案例/电商/a.sql"), Path("案例/电商/b.md")])
    pending = manager.get_pending_files()
    assert Path("案例/电商/a.sql") in pending
    assert Path("案例/电商/b.md") in pending
