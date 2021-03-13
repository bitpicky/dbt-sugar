from pathlib import Path, PosixPath
from unittest.mock import call

import pytest

from dbt_sugar.core.clients.dbt import DbtProfile
from dbt_sugar.core.task.base import COLUMN_NOT_DOCUMENTED
from dbt_sugar.core.task.doc import DocumentationTask

FIXTURE_DIR = Path(__file__).resolve().parent


def __init_descriptions(params=None, dbt_profile=None, config=None):
    doc_task = DocumentationTask(params, dbt_profile, config)
    doc_task.dbt_definitions = {"columnA": "descriptionA", "columnB": "descriptionB"}
    doc_task.repository_path = "tests/test_dbt_project/"
    return doc_task


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
    "content, model_name, ui_response, result",
    [
        (
            {
                "models": [
                    {
                        "name": "testmodel",
                        "columns": [
                            {
                                "name": "columnA",
                                "description": "descriptionA",
                                "tests": ["unique"],
                                "tags": ["hi", "hey"],
                            },
                            {"name": "columnF", "description": "descriptionF"},
                        ],
                    }
                ]
            },
            "testmodel",
            {
                "columnA": {
                    "description": "this is the description",
                    "tags": ["hi", "you"],
                    "test": ["unique", "not_null"],
                },
            },
            [
                call(
                    PosixPath("."),
                    {
                        "models": [
                            {
                                "name": "testmodel",
                                "columns": [
                                    {
                                        "name": "columnA",
                                        "description": "this is the description",
                                        "tests": ["unique"],
                                        "tags": ["hi", "you", "hey"],
                                    },
                                    {"name": "columnF", "description": "descriptionF"},
                                ],
                            }
                        ]
                    },
                )
            ],
        ),
    ],
)
def test_update_model_description_test_tags(mocker, content, model_name, ui_response, result):
    open_yaml = mocker.patch("dbt_sugar.core.task.base.open_yaml")
    save_yaml = mocker.patch("dbt_sugar.core.task.base.save_yaml")
    open_yaml.return_value = content
    doc_task = DocumentationTask(None, None, None)
    doc_task.update_model_description_test_tags(Path("."), model_name, ui_response)
    save_yaml.assert_has_calls(result)


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


@pytest.mark.parametrize(
    "content, model_name, result",
    [
        (
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
            "testmodel",
            {"columnA": "descriptionA", "columnB": "descriptionB"},
        ),
        (
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
            "testmodel1",
            {},
        ),
    ],
)
def test_get_documented_columns(content, model_name, result):
    doc_task = __init_descriptions()
    assert doc_task.get_documented_columns(content, model_name) == result


@pytest.mark.parametrize(
    "content, model_name, result",
    [
        (
            {
                "models": [
                    {
                        "name": "testmodel",
                        "description": "No description for this model.",
                        "columns": [
                            {"name": "columnA", "description": "descriptionA"},
                            {"name": "columnB", "description": COLUMN_NOT_DOCUMENTED},
                            {"name": "columnC", "description": COLUMN_NOT_DOCUMENTED},
                        ],
                    }
                ]
            },
            "testmodel",
            {"columnB": COLUMN_NOT_DOCUMENTED, "columnC": COLUMN_NOT_DOCUMENTED},
        ),
        (
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
            "testmodel1",
            {},
        ),
    ],
)
def test_get_documented_columns(content, model_name, result):
    doc_task = __init_descriptions()
    assert doc_task.get_not_documented_columns(content, model_name) == result


@pytest.mark.parametrize(
    "content, model_name, result",
    [
        (
            {
                "models": [
                    {
                        "name": "testmodel",
                        "description": "No description for this model.",
                        "columns": [],
                    }
                ]
            },
            "testmodel",
            {
                "models": [
                    {
                        "name": "testmodel",
                        "description": "New description for the model.",
                        "columns": [],
                    }
                ]
            },
        ),
        (
            {
                "models": [
                    {
                        "name": "testmodel",
                        "columns": [],
                    }
                ]
            },
            "testmodel",
            {
                "models": [
                    {
                        "name": "testmodel",
                        "description": "New description for the model.",
                        "columns": [],
                    }
                ]
            },
        ),
    ],
)
def test_change_model_description(mocker, content, model_name, result):
    doc_task = __init_descriptions()
    mocker.patch(
        "questionary.prompt",
        return_value={
            "wants_to_document_model": True,
            "model_description": "New description for the model.",
        },
    )
    assert doc_task.change_model_description(content, model_name) == result


def test_document_columns(mocker):
    class Question:
        def __init__(self, return_value):
            self._return_value = return_value

        def ask(self):
            return self._return_value

    class MockDbtSugarConfig:
        config = {"always_enforce_tests": True, "always_add_tags": True}

    doc_task = DocumentationTask(None, None, MockDbtSugarConfig)
    doc_task.dbt_definitions = {"columnA": "descriptionA", "columnB": "descriptionB"}
    mocker.patch("questionary.prompt", return_value={"cols_to_document": ["columnA"]})
    mocker.patch("questionary.confirm", return_value=Question(False))
    mocker.patch(
        "questionary.text",
        return_value=Question(
            {
                "columnA": "newDescriptionA",
            }
        ),
    )
    doc_task.document_columns(doc_task.dbt_definitions)
    doc_task.dbt_definitions = {"columnA": "newDescriptionA", "columnB": "descriptionB"}


@pytest.mark.parametrize(
    "model_name, path_model, is_schema",
    [
        pytest.param(
            "my_first_dbt_model",
            Path("tests/test_dbt_project/dbt_sugar_test/models/example/schema.yml"),
            False,
        ),
        pytest.param(
            "not_exists",
            None,
            False,
        ),
    ],
)
def test_find_model_in_dbt(model_name, path_model, is_schema):
    doc_task = __init_descriptions()
    path_file, schema = doc_task.find_model_in_dbt(model_name)
    assert path_file == path_model
    assert schema == is_schema
