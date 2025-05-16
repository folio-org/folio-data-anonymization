import pytest
from unittest.mock import Mock
from plugins.anonymizer import (
    anonymize,
)


@pytest.fixture
def mock_users(mocker, postgresql):
    users_instance = Mock()
    users_instance.return_value = postgresql
    return users_instance


@pytest.fixture
def mock_data() -> list:
    return ["circulation", "users"]


def test_anonymize(mocker, mock_users, mock_data, caplog):
    mocker.patch(
        "plugins.anonymizer.Users",
        return_value=mock_users,
    )
    anonymize(data=mock_data)
    assert "Anonymizing users" in caplog.text
