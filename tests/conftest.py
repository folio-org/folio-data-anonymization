import json
import pathlib
import sys

import pytest

root_directory = pathlib.Path(__file__).parent.parent
dir = root_directory
sys.path.append(str(dir))


@pytest.fixture
def configurations():
    configuration = {}
    config_path = root_directory / "folio_data_anonymization/plugins/config"
    for file_path in config_path.glob("*.json"):
        config_contents = json.loads(file_path.read_text())
        configuration[file_path.stem] = config_contents
    return configuration
