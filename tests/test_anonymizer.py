import pytest
from folio_data_anonymization.anonymizer import construct_schema_names


@pytest.fixture()
def mock_schemas() -> list:
    return ["mod_circulation_storage", "mod_organization_storage", "mod_users"]


def test_construct_schema_names(mock_schemas):
    schema_names = construct_schema_names(tenant="diku", schemas=mock_schemas)
    assert schema_names.pop() == "diku_mod_users"
