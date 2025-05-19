import copy
import json
import pathlib

from folio_data_anonymization.git_plugins.utils import fake_jsonb


def test_org_fake_jsonb(configurations):
    with (pathlib.Path(__file__).parent / "fixtures/organization.json").open() as fo:
        organization = json.load(fo)

    original_org = copy.deepcopy(organization)
    for row in configurations.get("anonymize_organization_tables"):
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
