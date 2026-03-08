from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable


@dataclass
class TokenBudget:
    max_tokens: int
    reserve_tokens: int = 1500

    @property
    def usable_tokens(self) -> int:
        return max(self.max_tokens - self.reserve_tokens, 1)

    def estimate_tokens(self, text: str) -> int:
        # Conservative estimate for mixed Chinese/SQL text.
        return max(len(text) // 2, 1)

    def can_fit(self, blocks: Iterable[str]) -> bool:
        return sum(self.estimate_tokens(block) for block in blocks) <= self.usable_tokens
