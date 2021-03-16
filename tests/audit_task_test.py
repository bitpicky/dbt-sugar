from argparse import ArgumentParser
from pathlib import Path

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
def test_get_total_columns_descriptions_statistics(dbt_definitions, result):
    audit_task = __init_descriptions()
    audit_task.dbt_definitions = dbt_definitions
    assert audit_task.get_total_columns_descriptions_statistics() == result
