import pydantic
import pytest

from folio_data_anonymization.plugins.select_tables import (
    calculate_table_ranges,
    combine_table_counts,
    construct_anon_config,
    fetch_records_batch_for_table,
    trigger_anonymize_dag,
    fetch_record_counts_per_table,
    schemas_tables,
)


class MockSQLExecuteQueryOperator(pydantic.BaseModel):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.__dict__.update(kwargs)

    def execute(self, sql):
        kwargs = self.__dict__
        match kwargs["result_type"]:
            case "counts":
                return mock_result_set_count()
            case "selections":
                return mock_result_set_selections()
            case _:
                return []


def mock_result_set_count():
    return [
        (76, 'count'),
    ]


def mock_result_set_selections():
    return [(), (), ()]


@pytest.fixture
def mock_get_current_context(monkeypatch, mocker):
    def _context():
        context = mocker.stub(name="context")
        context.get = lambda *args: {}
        return context

    monkeypatch.setattr(
        'folio_data_anonymization.plugins.select_tables.get_current_context',
        _context,
    )


@pytest.fixture
def config():
    return {
        "anonymize_abc_tables": [
            {"table_name": "mod_table_a.one", "anonymize": {"jsonb": []}},
            {"table_name": "mod_table_b.two", "anonymize": {"jsonb": []}},
            {
                "table_name": "mod_table_c.three",
                "anonymize": {"jsonb": []},
                "set_to_empty": {"jsonb": []},
            },
        ]
    }


def test_fetch_record_counts(mocker, mock_get_current_context, config):
    mocker.patch(
        'folio_data_anonymization.plugins.select_tables.SQLExecuteQueryOperator',
        return_value=MockSQLExecuteQueryOperator(result_type="counts"),
    )
    mocker.patch(
        'folio_data_anonymization.plugins.select_tables.sql_count_file',
        return_value='folio_data_anonymization/plugins/sql/counts.sql',
    )

    table = schemas_tables(config, "diku")[0]
    count = fetch_record_counts_per_table(schema=table)
    assert count == 76


def test_anonymize_selections(mocker, mock_get_current_context, config, caplog):
    mocker.patch(
        'folio_data_anonymization.plugins.select_tables.SQLExecuteQueryOperator',
        return_value=MockSQLExecuteQueryOperator(result_type="selections"),
    )
    mocker.patch(
        'folio_data_anonymization.plugins.select_tables.sql_selections_file',
        return_value='folio_data_anonymization/plugins/sql/selections.sql',
    )

    tables = schemas_tables(config, "diku")
    assert "diku_mod_table_c.three" in tables

    counts = combine_table_counts(
        schema_table=tables, record_counts_per_table=[38, 74, 76]
    )
    assert counts["diku_mod_table_a.one"] == 38

    ranges = calculate_table_ranges(batch_size=10, counts=counts, schemas_tables=tables)

    assert ranges[0]["table"] == "diku_mod_table_a.one"
    assert ranges[0]["ranges"][0] == (0, 10)
    assert ranges[0]["ranges"][3] == (30, 40)

    for ranges_li in ranges:
        table = ranges_li["table"]
        table_config = construct_anon_config(config, table)
        data = fetch_records_batch_for_table(table, 0, 10)
        operator = trigger_anonymize_dag(data, table_config, "diku")
        assert operator.trigger_dag_id == "anonymize_data"
        assert operator.task_id == "trigger_anonymize_data_dag"
        assert (
            "Selecting records batch for table: diku_mod_table_a.one, offset: 0, limit: 10"  # noqa
        ) in caplog.text
        assert ("Triggered anonymize_data DAG for diku_mod_table_a.one") in caplog.text
