import pytest

from influx_mcp.config import ConfigError, load_config


def test_load_config_v2_only():
    env = {
        "INFLUX_VERSION": "2",
        "INFLUX_URL": "https://example.com",
        "INFLUX_ORG": "iot",
        "INFLUX_TOKEN": "token",
        "MCP_LOG_LEVEL": "debug",
    }
    config = load_config(env)
    assert config.v2 is not None
    assert config.v2.url == "https://example.com"
    assert config.influx_version == "2"
    assert config.log_level.lower() == "debug"


def test_load_config_v1_auto_defaults():
    env = {
        "INFLUX_URL": "http://localhost:8086",
        "INFLUX_USERNAME": "user",
        "INFLUX_PASSWORD": "pass",
        "INFLUX_DEFAULT_DB": "sensors",
    }
    config = load_config(env)
    assert config.v1 is not None
    assert config.v1.default_db == "sensors"
    assert config.influx_version == "auto"


def test_load_config_requires_values():
    env = {"INFLUX_VERSION": "2", "INFLUX_URL": "https://example.com"}
    with pytest.raises(ConfigError):
        load_config(env)


def test_invalid_version():
    with pytest.raises(ConfigError):
        load_config({"INFLUX_VERSION": "five"})
