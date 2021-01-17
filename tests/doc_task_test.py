from pathlib import Path

import pytest

from dbt_sugar.core.clients.dbt import DbtProfile
from dbt_sugar.core.task.base import COLUMN_NOT_DOCUMENTED
from dbt_sugar.core.task.doc import DocumentationTask

FIXTURE_DIR = Path(__file__).resolve().parent


def __init_descriptions(params=None, dbt_profile=None):
    doc_task = DocumentationTask(params, dbt_profile)
    doc_task.dbt_definitions = {"columnA": "descriptionA", "columnB": "descriptionB"}
    return doc_task


def test_load_dbt_credentials():
    profile = DbtProfile(
        profile_name="dbt_sugar_test",
        target_name="snowflake",
        profiles_dir=Path(FIXTURE_DIR).joinpath("profiles.yml"),
    )
    doc_task = __init_descriptions(None, profile)
    credentials = doc_task.load_dbt_credentials()
    assert credentials == {
        "type": "snowflake",
        "account": "dummy_account",
        "user": "dummy_user",
        "password": "dummy_password",
        "database": "dummy_database",
        "target_schema": "dummy_target_schema",
        "role": "dummy_role",
        "warehouse": "dummy_warehouse",
    }


@pytest.mark.parametrize(
    "column, result",
    [
        (
            "columnA",
            "descriptionA",
        ),
        (
            "columnD",
            COLUMN_NOT_DOCUMENTED,
        ),
    ],
)
def test_get_column_description_from_dbt_definitions(column, result):
    doc_task = __init_descriptions()
    assert doc_task.get_column_description_from_dbt_definitions(column) == result


@pytest.mark.parametrize(
    "column, description, result",
    [
        (
            "columnC",
            "descriptionC",
            "descriptionC",
        ),
        (
            "columnD",
            None,
            COLUMN_NOT_DOCUMENTED,
        ),
    ],
)
def test_update_description_in_dbt_descriptions(column, description, result):
    doc_task = __init_descriptions()
    doc_task.update_description_in_dbt_descriptions(column, description)
    assert doc_task.dbt_definitions[column] == result


@pytest.mark.parametrize(
    "content, column, description",
    [
        (
            {
                "models": [
                    {
                        "name": "testmodel",
                        "columns": [
                            {"name": "columnE", "description": "descriptionE"},
                            {"name": "columnF", "description": "descriptionF"},
                        ],
                    }
                ]
            },
            "columnE",
            "descriptionE",
        ),
        (
            {
                "models": [
                    {
                        "name": "testmodel",
                        "columns": [
                            {"name": "columnE", "description": "descriptionE"},
                            {"name": "columnF", "description": "descriptionF"},
                        ],
                    }
                ]
            },
            "columnF",
            "descriptionF",
        ),
    ],
)
def test_save_descriptions_from_schema(content, column, description):
    doc_task = __init_descriptions()
    doc_task.save_descriptions_from_schema(content)
    assert doc_task.dbt_definitions[column] == description


@pytest.mark.parametrize(
    "content, model_name, result",
    [
        (
            {
                "models": [
                    {
                        "name": "testmodel",
                        "columns": [],
                    },
                    {
                        "name": "testmodel1",
                        "columns": [],
                    },
                ]
            },
            "testmodel",
            True,
        ),
        (
            {
                "models": [
                    {
                        "name": "testmodel1",
                        "columns": [],
                    },
                ]
            },
            "testmodel1",
            True,
        ),
        (
            {
                "models": [
                    {
                        "name": "testmodel1",
                        "columns": [],
                    },
                ]
            },
            "testmodel2",
            False,
        ),
    ],
)
def test_is_model_in_schema_content(content, model_name, result):
    doc_task = __init_descriptions()
    assert doc_task.is_model_in_schema_content(content, model_name) == result
    assert doc_task.is_model_in_schema_content(content, model_name) == result
    assert doc_task.is_model_in_schema_content(content, model_name) == result


@pytest.mark.parametrize(
    "content, model_name, columns_sql, result",
    [
        (
            {
                "models": [
                    {
                        "name": "testmodel",
                        "columns": [{"name": "columnB", "description": "descriptionX"}],
                    }
                ]
            },
            "testmodel",
            ["columnA", "columnB", "columnC"],
            {
                "models": [
                    {
                        "name": "testmodel",
                        "columns": [
                            {"description": "descriptionX", "name": "columnB"},
                            {"description": "descriptionA", "name": "columnA"},
                            {"description": "No description for this column.", "name": "columnC"},
                        ],
                    }
                ]
            },
        ),
    ],
)
def test_update_model(content, model_name, columns_sql, result):
    doc_task = __init_descriptions()
    assert doc_task.update_model(content, model_name, columns_sql) == result


@pytest.mark.parametrize(
    "content, model_name, columns_sql, result",
    [
        (
            {"models": []},
            "testmodel",
            ["columnA", "columnB", "columnC"],
            {
                "models": [
                    {
                        "name": "testmodel",
                        "description": "No description for this model.",
                        "columns": [
                            {"name": "columnA", "description": "descriptionA"},
                            {"name": "columnB", "description": "descriptionB"},
                            {"name": "columnC", "description": COLUMN_NOT_DOCUMENTED},
                        ],
                    }
                ]
            },
        ),
        (
            None,
            "testmodel",
            ["columnA", "columnB", "columnC"],
            {
                "version": 2,
                "models": [
                    {
                        "name": "testmodel",
                        "description": "No description for this model.",
                        "columns": [
                            {"name": "columnA", "description": "descriptionA"},
                            {"name": "columnB", "description": "descriptionB"},
                            {"name": "columnC", "description": COLUMN_NOT_DOCUMENTED},
                        ],
                    }
                ],
            },
        ),
    ],
)
def test_create_new_model(content, model_name, columns_sql, result):
    doc_task = __init_descriptions()
    assert doc_task.create_new_model(content, model_name, columns_sql) == result
