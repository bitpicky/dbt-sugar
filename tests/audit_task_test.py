from argparse import ArgumentParser
from pathlib import Path
from unittest.mock import call

import pytest

from dbt_sugar.core.task.audit import AuditTask
from dbt_sugar.core.task.base import COLUMN_NOT_DOCUMENTED

FIXTURE_DIR = Path(__file__).resolve().parent


def __init_descriptions():
    parser = ArgumentParser()
    parser.add_argument("-m", "--model", default=None)
    params = parser.parse_args([])

    audit_task = AuditTask(params)
    audit_task.dbt_definitions = {"columnA": "descriptionA", "columnB": "descriptionB"}
    audit_task.repository_path = "tests/test_dbt_project/"
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
    assert audit_task.calculate_coverage_percentage(number_failures=failures, total=total) == result


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
            {"column_A": "10.0"},
            id="check_results_with_one_data_element",
        ),
        pytest.param(
            ["column_A", "column_B"],
            "10.0",
            {"column_A": "", "column_B": "10.0"},
            id="check_results_with_more_than_one_data_element",
        ),
    ],
)
def test_print_nicely_the_data(data, total, result):
    audit_task = __init_descriptions()
    assert audit_task.print_nicely_the_data(data=data, total=total) == result
