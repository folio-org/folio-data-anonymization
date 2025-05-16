from faker import Faker
from jsonpath_ng import parse

from plugins.providers import Organizations

faker = Faker()
faker.add_provider(Organizations)


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
