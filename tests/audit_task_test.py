from pathlib import Path
from unittest.mock import call

import pytest

from dbt_sugar.core.config.config import DbtSugarConfig
from dbt_sugar.core.flags import FlagParser
from dbt_sugar.core.main import parser
from dbt_sugar.core.task.audit import AuditTask
from dbt_sugar.core.task.base import COLUMN_NOT_DOCUMENTED

FIXTURE_DIR = Path(__file__).resolve().parent


def __init_descriptions():
    flag_parser = FlagParser(parser)
    config_filepath = Path(FIXTURE_DIR).joinpath("sugar_config.yml")
    flag_parser.consume_cli_arguments(
        test_cli_args=[
            "audit",
            "--config-path",
            str(config_filepath),
        ]
    )
    sugar_config = DbtSugarConfig(flag_parser)
    sugar_config.load_config()
    audit_task = AuditTask(flag_parser, FIXTURE_DIR, sugar_config=sugar_config)
    audit_task.dbt_definitions = {"columnA": "descriptionA", "columnB": "descriptionB"}
    audit_task.repository_path = Path("tests/test_dbt_project/")
    return audit_task


@pytest.mark.parametrize(
    "dbt_definitions, result",
    [
        pytest.param(
            {"columnA": "descriptionA", "columnB": "descriptionB"},
            "100.0",
            id="all_columns_documented",
        ),
        pytest.param(
            {"columnA": COLUMN_NOT_DOCUMENTED, "columnB": COLUMN_NOT_DOCUMENTED},
            "0.0",
            id="none_columns_documented",
        ),
        pytest.param(
            {"columnA": "descriptionA", "columnB": COLUMN_NOT_DOCUMENTED},
            "50.0",
            id="half_columns_documented",
        ),
    ],
)
def test_get_project_total_test_coverage(dbt_definitions, result):
    audit_task = __init_descriptions()
    audit_task.dbt_definitions = dbt_definitions
    assert audit_task.get_project_total_test_coverage() == result


@pytest.mark.parametrize(
    "failures, total, result",
    [
        pytest.param(
            0,
            0,
            "0.0",
            id="calculate_failures_with_0_failures_and_total",
        ),
        pytest.param(
            8,
            10,
            "20.0",
            id="calculate_failures",
        ),
        pytest.param(
            0,
            10,
            "100.0",
            id="calculate_failures_with_0_failures",
        ),
    ],
)
def test_calculate_coverage_percentage(failures, total, result):
    audit_task = __init_descriptions()
    assert audit_task.calculate_coverage_percentage(misses=failures, total=total) == result


@pytest.mark.parametrize(
    "data, total, result",
    [
        pytest.param(
            [],
            "0.0",
            {},
            id="check_results_with_data_being_empty",
        ),
        pytest.param(
            ["column_A"],
            "10.0",
            {"column_A": "", "": "", "Total": "10.0"},
            id="check_results_with_one_data_element",
        ),
        pytest.param(
            ["column_A", "column_B"],
            "10.0",
            {"column_A": "", "column_B": "", "": "", "Total": "10.0"},
            id="check_results_with_more_than_one_data_element",
        ),
    ],
)
def test_print_nicely_the_data(data, total, result):
    audit_task = __init_descriptions()
    assert audit_task.print_nicely_the_data(data=data, total=total) == result


@pytest.mark.parametrize(
    "dbt_tests, model_name, call_input",
    [
        pytest.param(
            {
                "dim_company": [
                    {"name": "id", "tests": []},
                    {"name": "name", "tests": []},
                    {"name": "age", "tests": []},
                    {"name": "address", "tests": ["not_null"]},
                    {"name": "salary", "tests": ["unique"]},
                ],
                "stg_customers": [{"name": "customer_id", "tests": ["unique", "not_null"]}],
            },
            "dim_company",
            [
                call(
                    columns=["Untested Columns", "% coverage"],
                    data={"age": "", "id": "", "name": "", "": "", "Total": "40.0"},
                    title="Test Coverage",
                )
            ],
            id="check_test_coverage_calculation",
        ),
    ],
)
def test_get_model_test_coverage(mocker, dbt_tests, model_name, call_input):
    create_table = mocker.patch("dbt_sugar.core.task.audit.AuditTask.create_table")
    audit_task = __init_descriptions()
    audit_task.model_name = model_name
    audit_task.dbt_tests = dbt_tests

    audit_task.get_model_test_coverage()
    create_table.assert_has_calls(call_input)


@pytest.mark.parametrize(
    "dbt_tests, call_input",
    [
        pytest.param(
            {
                "dim_company": [
                    {"name": "id", "tests": []},
                    {"name": "name", "tests": []},
                    {"name": "age", "tests": []},
                    {"name": "address", "tests": ["not_null"]},
                    {"name": "salary", "tests": ["unique"]},
                ],
                "stg_customers": [{"name": "customer_id", "tests": ["unique", "not_null"]}],
            },
            [
                call(
                    columns=["Model Name", "% coverage"],
                    data={"dim_company": "40.0", "stg_customers": "100.0", "": "", "Total": "50.0"},
                    title="Test Coverage",
                )
            ],
            id="check_test_coverage_calculation",
        ),
    ],
)
def test_get_project_test_coverage(mocker, dbt_tests, call_input):
    create_table = mocker.patch("dbt_sugar.core.task.audit.AuditTask.create_table")
    audit_task = __init_descriptions()
    audit_task.dbt_tests = dbt_tests

    audit_task.get_project_test_coverage()
    create_table.assert_has_calls(call_input)


@pytest.mark.parametrize(
    "model_content, model_name, call_input",
    [
        pytest.param(
            {
                "version": 2,
                "models": [
                    {
                        "name": "dim_company",
                        "description": "aa.",
                        "columns": [
                            {"name": "id", "description": "No description for this column."},
                            {"name": "name", "description": "No description for this column."},
                            {"name": "age", "description": "No description for this column."},
                            {
                                "name": "address",
                                "description": "No description for this column.",
                                "tests": ["not_null"],
                            },
                            {"name": "salary", "description": "hey.", "tests": ["unique"]},
                        ],
                    }
                ],
            },
            "dim_company",
            [
                call(
                    columns=["Undocumented Columns", "% coverage"],
                    data={"id": "", "name": "", "age": "", "address": "", "": "", "Total": "20.0"},
                    title="Documentation Coverage",
                )
            ],
            id="check_column_description_coverage_calculation",
        ),
    ],
)
def test_get_model_column_description_coverage(mocker, model_content, model_name, call_input):
    audit_task = __init_descriptions()
    audit_task.get_model_column_description_coverage()

    create_table = mocker.patch("dbt_sugar.core.task.audit.AuditTask.create_table")
    audit_task = __init_descriptions()
    audit_task.model_content = model_content
    audit_task.model_name = model_name

    audit_task.get_model_column_description_coverage()
    create_table.assert_has_calls(call_input)
