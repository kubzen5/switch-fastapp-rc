import pytest

from app import config


def test_invalid_settings_do_not_expose_unvalidated_secrets(monkeypatch):
    original_settings = config.Settings
    monkeypatch.setattr(config, "Settings", lambda: original_settings(
        _env_file=None, postgres_password="", snowflake_password="sensitive-test-value"
    ))
    config.get_settings.cache_clear()
    with pytest.raises(RuntimeError, match="Invalid environment configuration") as caught:
        config.get_settings()
    assert "sensitive-test-value" not in str(caught.value)
    assert caught.value.__suppress_context__
