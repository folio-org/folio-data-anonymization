import pydantic
import pytest

from plugins.truncate import tables_list, truncate_db_objects


class MockSQLExecuteQueryOperator(pydantic.BaseModel):
    def execute(self, sql):
        return mock_result_set()


def mock_result_set():
    return (
        ('TRUNCATE'),
        ('TRUNCATE'),
        ('TRUNCATE'),
    )


@pytest.fixture
def mock_get_current_context(monkeypatch, mocker):
    def _context():
        context = mocker.stub(name="context")
        context.get = lambda *args: {}
        return context

    monkeypatch.setattr(
        'plugins.truncate.get_current_context',
        _context,
    )


def setup_tests(mocker):
    mocker.patch(
        'plugins.truncate.SQLExecuteQueryOperator',
        return_value=MockSQLExecuteQueryOperator(),
    )
    mocker.patch(
        'plugins.truncate.tables_json_file',
        return_value='tests/fixtures/mock_truncate_schemas_tables.json',
    )
    mocker.patch(
        'plugins.truncate.sql_file',
        return_value='plugins/sql/truncate_schemas_tables.sql',
    )


def test_truncate_tables(mocker, mock_get_current_context, caplog):
    setup_tests(mocker)

    tables = tables_list()
    results = truncate_db_objects(schemas_tables=tables)
    assert (
        "Will truncate diku_mod_schema_one.table,diku_mod_schema_two.table,diku_mod_schema_tree.table"  # noqa
        in caplog.text
    )
    assert "TRUNCATE" in results
