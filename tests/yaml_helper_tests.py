import os
import tempfile
from pathlib import Path

import pytest
import yaml

from dbt_sugar.core.clients.yaml_helpers import open_yaml, save_yaml


@pytest.fixture
def create_temp_schema_yaml(tmp_path):
    file_name = tmp_path / "schema.yml"
    schema_content = """
    {
        "models": [
            {"name": "model1", "columns": [{"name": "column1", "description": "description1"}]}
        ]
    }"""
    file_name.write_text(schema_content)
    return file_name


@pytest.fixture
def create_tmp_empty_schema_yaml(tmp_path):
    file_name = tmp_path / "schema.yml"
    schema_content = ""
    file_name.write_text(schema_content)
    return file_name


def test_open_yaml(create_temp_schema_yaml):
    result = open_yaml(create_temp_schema_yaml)
    assert result == {
        "models": [
            {"name": "model1", "columns": [{"name": "column1", "description": "description1"}]}
        ]
    }


def test_open_yaml_empty_file(create_tmp_empty_schema_yaml):
    from dbt_sugar.core.exceptions import YAMLFileEmptyError

    with pytest.raises(YAMLFileEmptyError):
        _ = open_yaml(create_tmp_empty_schema_yaml)


def test_open_yaml_no_file():
    with pytest.raises(FileNotFoundError):
        _ = open_yaml(Path("no_file"))


@pytest.mark.parametrize(
    "content, result",
    (
        (
            {"models": [{"columns": [], "name": "model1"}]},
            """{"models": [{"columns": [], "name": "model1"}]}""",
        ),
    ),
)
def test_save_yaml(content, result):
    temp_file = tempfile.NamedTemporaryFile(delete=False)
    try:
        save_yaml(temp_file.name, content)
        yaml_content = open_yaml(Path(temp_file.name))
        assert yaml_content == yaml.safe_load(result)
    finally:
        os.unlink(temp_file.name)
        temp_file.close()
