import copy
import json
import pathlib

from folio_data_anonymization.plugins.utils import fake_jsonb


def test_org_fake_jsonb(configs):
    with (pathlib.Path(__file__).parent / "fixtures/organization.json").open() as fo:
        organization = json.load(fo)

    original_org = copy.deepcopy(organization)
    for row in configs.get("anonymize_organization_tables"):
        if row['table_name'].startswith("mod_organizations_storage.organizations"):
            org_config = row

    fake_jsonb(organization, org_config)

    assert organization["name"] != original_org["name"]
    assert organization["edi"]["libEdiType"] != original_org["edi"]["libEdiType"]
    assert (
        organization["addresses"][0]["addressLine1"]
        != original_org["addresses"][0]["addressLine1"]
    )
    assert organization["addresses"][0]["city"] != original_org["addresses"][0]["city"]
    assert organization["emails"][0]["value"] != original_org["emails"][0]["value"]
    assert len(organization["aliases"][0]["description"]) == 0


def test_user_fake_jsonb(configs):
    with (pathlib.Path(__file__).parent / "fixtures/user.json").open() as fo:
        user = json.load(fo)

    original_user = copy.deepcopy(user)
    for row in configs.get("anonymize_users_tables"):
        if row['table_name'].startswith("mod_users.users"):
            user_config = row

    fake_jsonb(user, user_config)

    assert user["username"] != original_user["username"]
    assert user["barcode"] != original_user["barcode"]
    assert user["externalSystemId"] != original_user["externalSystemId"]
    assert user["personal"]["lastName"] != original_user["personal"]["lastName"]
    assert (
        user["personal"]["addresses"][0]["addressLine1"]
        != original_user["personal"]["addresses"][0]["addressLine1"]
    )
    assert len(user.get('customFields')) == 0
