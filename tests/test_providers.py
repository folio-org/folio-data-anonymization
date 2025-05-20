import pytest

from faker import Faker

from folio_data_anonymization.plugins.providers import Organizations


@pytest.fixture
def custom_providers():
    faker = Faker()
    faker.add_provider(Organizations)
    return faker


def test_edi_type(custom_providers):
    fake_edi_type = custom_providers.edi_type()
    assert isinstance(fake_edi_type, str)
    assert len(fake_edi_type) >= 5
    assert fake_edi_type[3] == "/"
    assert fake_edi_type[-1].isalpha()
    assert fake_edi_type[-4] == "-"


def test_org_code(custom_providers):
    fake_org_code = custom_providers.org_code()
    assert isinstance(fake_org_code, str)
    assert len(fake_org_code) >= 3
    assert all(c.isupper() or c == "-" for c in fake_org_code)
