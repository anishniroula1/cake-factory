import pytest


@pytest.fixture
def config_stub():
    class ConfigStub:
        pass

    return ConfigStub()
