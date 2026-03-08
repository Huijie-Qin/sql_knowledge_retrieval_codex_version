from src.token_budget import TokenBudget


def test_estimate_tokens_and_fit_budget():
    budget = TokenBudget(max_tokens=8000, reserve_tokens=1500)
    assert budget.estimate_tokens("a" * 4000) > 0
    assert budget.can_fit(["a" * 1000, "b" * 1000]) is True
