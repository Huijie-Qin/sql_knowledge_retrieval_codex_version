from pathlib import Path

from config.settings import Settings


def test_settings_load_from_env(monkeypatch):
    monkeypatch.setenv("SOURCE_DIR", "./案例")
    monkeypatch.setenv("OUTPUT_DIR", "./自定义数据源")
    monkeypatch.setenv("MAX_CONTEXT_TOKENS", "64000")

    settings = Settings()
    assert settings.source_dir == Path("./案例")
    assert settings.output_dir == Path("./自定义数据源")
    assert settings.max_context_tokens == 64000
