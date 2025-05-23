from faker import Faker
from jsonpath_ng import parse

try:
    from plugins.git_plugins.providers import Organizations, Users
except (ImportError, ModuleNotFoundError):
    from folio_data_anonymization.plugins.providers import Organizations, Users

faker = Faker()
faker.add_provider(Organizations)
faker.add_provider(Users)


def fake_jsonb(jsonb: dict, config: dict) -> dict:
    """
    Fake the jsonb data based on the provided config.
    """
    for row in config["anonymize"]["jsonb"]:
        expr = parse(row[0])
        faker_function = getattr(faker, row[1])
        expr.update(jsonb, faker_function())
    for row in config.get("set_to_empty", {}).get("jsonb", []):
        expr = parse(row)
        expr.update(jsonb, "")
    return jsonb
