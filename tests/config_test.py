from tests.profile_test import FIXTURE_DIR
import pytest
from pathlib import Path

FIXTURE_DIR = Path(__file__).resolve().parent


@pytest.mark.parametrize(
    "cli_args, test_desc",
    [
        (["doc", "-m", "test_model"], "no_log_level"),
        (["doc", "-m", "test_model", "--log-level", "debug"], "log_debug"),
        (
            ["not_allowed_command", "-m", "test_model", "--log-level", "debug"],
            "non_allowed_command",
        ),
    ],
)
def test_Config(cli_args, test_desc):
    from dbt_sugar.core.config.config import DbtSugarConfig
    from dbt_sugar.core.flags import FlagParser
    from dbt_sugar.core.main import parser
    from dbt_sugar.core.logger import GLOBAL_LOGGER as logger

    flag_parser = FlagParser(parser)
    if test_desc == "non_allowed_command":
        with pytest.raises(SystemExit):
            flag_parser.consume_cli_arguments(test_cli_args=cli_args)
    else:
        flag_parser.consume_cli_arguments(test_cli_args=cli_args)

    if test_desc != "non_allowed_command":
        config = DbtSugarConfig(flag_parser)
        assert config._model_name == cli_args[2]
        assert config._task == cli_args[0]
        if test_desc == "log_debug":
            assert logger.level == 10  # debug level is 10


@pytest.mark.parametrize(
    "has_no_default_syrup, is_missing_syrup, is_missing_dbt_project",
    [(False, False, False), (True, False, False), (False, True, False), (False, False, True)],
)
@pytest.mark.datafiles(FIXTURE_DIR)
def test_load_config(datafiles, has_no_default_syrup, is_missing_syrup, is_missing_dbt_project):
    from dbt_sugar.core.main import parser
    from dbt_sugar.core.flags import FlagParser
    from dbt_sugar.core.config.config import DbtSugarConfig
    from dbt_sugar.core.exceptions import (
        SyrupNotFoundError,
        NoSyrupProvided,
        MissingDbtProjects,
    )

    expectation = {
        "name": "syrup_1",
        "dbt_projects": [
            {
                "name": "dbt_sugar_test",
                "path": "./tests/test_dbt_project/dbt_sugar_test",
                "excluded_tables": ["table_a"],
            }
        ],
    }

    config_filepath = Path(datafiles).joinpath("sugar_config.yml")
    if has_no_default_syrup:
        config_filepath = Path(datafiles).joinpath("sugar_config_missing_default.yml")

    if is_missing_syrup:
        cli_args = ["doc", "--config-path", str(config_filepath), "--syrup", "non_existant"]
    elif has_no_default_syrup:
        cli_args = ["doc", "--config-path", str(config_filepath)]
    elif is_missing_dbt_project:
        cli_args = ["doc", "--config-path", str(config_filepath), "--syrup", "syrup_2"]
    else:
        cli_args = ["doc", "--config-path", str(config_filepath)]

    flags = FlagParser(parser)
    flags.consume_cli_arguments(cli_args)

    config = DbtSugarConfig(flags)
    if is_missing_syrup:
        with pytest.raises(SyrupNotFoundError):
            config.load_config()
    elif has_no_default_syrup:
        with pytest.raises(NoSyrupProvided):
            config.load_config()
    elif is_missing_dbt_project:
        with pytest.raises(MissingDbtProjects):
            config.load_config()
    else:
        config.load_config()
        assert config.config == expectation
