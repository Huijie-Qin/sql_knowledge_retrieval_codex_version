from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, Iterable, Set


class TableUsageTracker:
    def __init__(self, file_path: Path) -> None:
        self.file_path = file_path
        self.file_path.parent.mkdir(parents=True, exist_ok=True)
        self._index: Dict[str, Set[str]] = {}
        self._load()

    def _load(self) -> None:
        if not self.file_path.exists():
            self._index = {}
            return
        raw = json.loads(self.file_path.read_text(encoding="utf-8"))
        self._index = {key: set(values) for key, values in raw.items()}

    def _save(self) -> None:
        serializable = {key: sorted(values) for key, values in sorted(self._index.items())}
        self.file_path.write_text(
            json.dumps(serializable, ensure_ascii=False, indent=2), encoding="utf-8"
        )

    def update(self, source_file: str, table_names: Iterable[str]) -> None:
        normalized = {name.strip() for name in table_names if name and name.strip()}
        self._index[source_file] = normalized
        self._save()

    def get_tables_for_file(self, source_file: str) -> Set[str]:
        return set(self._index.get(source_file, set()))

    def get_all_used_tables(self) -> Set[str]:
        tables: Set[str] = set()
        for group in self._index.values():
            tables.update(group)
        return tables
