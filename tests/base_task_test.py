from pathlib import Path
from unittest.mock import call

import pytest

from dbt_sugar.core.clients.dbt import DbtProfile
from dbt_sugar.core.config.config import DbtSugarConfig
from dbt_sugar.core.flags import FlagParser
from dbt_sugar.core.main import parser
from dbt_sugar.core.task.audit import AuditTask

FIXTURE_DIR = Path(__file__).resolve().parent


def __init_descriptions(datafiles):
    flag_parser = FlagParser(parser)
    config_filepath = Path(FIXTURE_DIR).joinpath("sugar_config.yml")
    flag_parser.consume_cli_arguments(
        test_cli_args=["audit", "--config-path", str(config_filepath)]
    )
    sugar_config = DbtSugarConfig(flag_parser)
    sugar_config.load_config()

    profile = DbtProfile(
        flags=flag_parser,
        profile_name="dbt_sugar_test",
        target_name=str(),
        profiles_dir=Path(datafiles),
    )
    profile.read_profile()

    dbt_project_path = Path(FIXTURE_DIR).joinpath("test_dbt_project/dbt_sugar_test")
    audit_task = AuditTask(
        flag_parser, dbt_project_path, sugar_config=sugar_config, dbt_profile=profile
    )
    audit_task.dbt_definitions = {"columnA": "descriptionA", "columnB": "descriptionB"}
    return audit_task


@pytest.mark.parametrize(
    "model_name, is_exluded_model",
    [
        pytest.param("my_first_dbt_model", False, id="model is not excluded"),
        pytest.param("my_first_dbt_model_excluded", True, id="model is not excluded"),
    ],
)
@pytest.mark.datafiles(FIXTURE_DIR)
def test_is_excluded_model(datafiles, model_name, is_exluded_model):
    audit_task = __init_descriptions(datafiles)
    if is_exluded_model:
        with pytest.raises(ValueError):
            audit_task.is_excluded_model(model_name)
    else:
        audit_task.is_excluded_model(model_name)


@pytest.mark.parametrize(
    "model_path, schema_suffix",
    [
        pytest.param("models/example/any_model", "", id="model is in base schema"),
        pytest.param(
            "models/example/custom/any_other_model", "_custom", id="model is in custom schema"
        ),
    ],
)
@pytest.mark.datafiles(FIXTURE_DIR)
def test_get_appropriate_schema_suffix(datafiles, model_path, schema_suffix):
    audit_task = __init_descriptions(datafiles)
    assert audit_task.get_appropriate_schema_suffix(model_path) == schema_suffix
