from pathlib import Path

import pytest

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
    from dbt_sugar.core.logger import GLOBAL_LOGGER as logger
    from dbt_sugar.core.main import parser

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
    from dbt_sugar.core.config.config import DbtSugarConfig
    from dbt_sugar.core.exceptions import MissingDbtProjects, NoSyrupProvided, SyrupNotFoundError
    from dbt_sugar.core.flags import FlagParser
    from dbt_sugar.core.main import parser

    expectation = {
        "name": "syrup_1",
        "dbt_projects": [
            {
                "name": "dbt_sugar_test",
                "path": "./tests/test_dbt_project/dbt_sugar_test",
                "excluded_models": ["table_a"],
                "excluded_folders": ["folder_to_exclude"],
            },
        ],
        "always_add_tags": True,
        "always_enforce_tests": True,
    }

    config_filepath = Path(datafiles).joinpath("sugar_config.yml")
    if has_no_default_syrup:
        config_filepath = Path(datafiles).joinpath("sugar_config_missing_default.yml")

    if is_missing_syrup:
        cli_args = [
            "doc",
            "--config-path",
            str(config_filepath),
            "--syrup",
            "non_existant",
            "-m",
            "test_model",
        ]
    elif has_no_default_syrup:
        cli_args = ["doc", "--config-path", str(config_filepath), "-m", "test_model"]
    elif is_missing_dbt_project:
        cli_args = [
            "doc",
            "--config-path",
            str(config_filepath),
            "--syrup",
            "syrup_2",
            "-m",
            "test_model",
        ]
    else:
        cli_args = ["doc", "-m", "test_model"]

    flags = FlagParser(parser)
    flags.consume_cli_arguments(cli_args)

    config = DbtSugarConfig(flags)

    # patch the current folder to be the one set by pytest datafile fixture
    config._current_folder = Path(datafiles)

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
        assert config._config_file_found_nearby is True


@pytest.mark.parametrize(
    "config_input, expect_single_project",
    [
        pytest.param(
            {
                "name": "syrup_1",
                "dbt_projects": [
                    {
                        "name": "dbt_sugar_test",
                        "path": "./tests/test_dbt_project/dbt_sugar_test",
                        "excluded_models": ["table_a"],
                    }
                ],
            },
            True,
            id="single_dbt_project",
        ),
        pytest.param(
            {
                "name": "syrup_1",
                "dbt_projects": [
                    {
                        "name": "dbt_sugar_test",
                        "path": "./tests/test_dbt_project/dbt_sugar_test",
                        "excluded_models": ["table_a"],
                    },
                    {
                        "name": "dbt_sugar_test_2",
                        "path": "./tests/test_dbt_project/dbt_sugar_test",
                        "excluded_models": ["table_a"],
                    },
                ],
            },
            False,
            id="multiple_dbt_projects",
        ),
    ],
)
@pytest.mark.datafiles(FIXTURE_DIR)
def test_assert_only_one_dbt_project_in_scope(
    monkeypatch, datafiles, config_input, expect_single_project
):
    from dbt_sugar.core.config.config import DbtSugarConfig
    from dbt_sugar.core.exceptions import KnownRegressionError
    from dbt_sugar.core.flags import FlagParser
    from dbt_sugar.core.main import parser

    cli_args = ["doc", "-m", "test_model", "--config-path", str(datafiles)]
    flags_parser = FlagParser(cli_parser=parser)
    flags_parser.consume_cli_arguments(test_cli_args=cli_args)

    config = DbtSugarConfig(flags_parser)
    monkeypatch.setattr(DbtSugarConfig, "config", config_input)

    if expect_single_project is True:
        is_good = config.assert_only_one_dbt_project_in_scope()
        assert is_good is True
    else:
        with pytest.raises(KnownRegressionError):
            _ = config.assert_only_one_dbt_project_in_scope()


@pytest.mark.parametrize(
    "test_and_flag_args, expectation",
    [
        pytest.param(
            "",
            {
                "name": "syrup_1",
                "dbt_projects": [
                    {
                        "name": "dbt_sugar_test",
                        "path": "./tests/test_dbt_project/dbt_sugar_test",
                        "excluded_models": ["table_a"],
                        "excluded_folders": ["folder_to_exclude"],
                    }
                ],
                "always_enforce_tests": True,
                "always_add_tags": True,
            },
            id="no_test_or_tag_override",
        ),
        pytest.param(
            "--no-ask-tests",
            {
                "name": "syrup_1",
                "dbt_projects": [
                    {
                        "name": "dbt_sugar_test",
                        "path": "./tests/test_dbt_project/dbt_sugar_test",
                        "excluded_models": ["table_a"],
                        "excluded_folders": ["folder_to_exclude"],
                    }
                ],
                "always_enforce_tests": False,
                "always_add_tags": True,
            },
            id="no_tests_on_cli",
        ),
        pytest.param(
            "--no-ask-tags",
            {
                "name": "syrup_1",
                "dbt_projects": [
                    {
                        "name": "dbt_sugar_test",
                        "path": "./tests/test_dbt_project/dbt_sugar_test",
                        "excluded_models": ["table_a"],
                        "excluded_folders": ["folder_to_exclude"],
                    }
                ],
                "always_enforce_tests": True,
                "always_add_tags": False,
            },
            id="no_tags_on_cli",
        ),
    ],
)
@pytest.mark.datafiles(FIXTURE_DIR)
def test__integrate_cli_flags(datafiles, test_and_flag_args, expectation):
    from dbt_sugar.core.config.config import DbtSugarConfig
    from dbt_sugar.core.flags import FlagParser
    from dbt_sugar.core.main import parser

    config_filepath = Path(datafiles).joinpath("sugar_config.yml")
    cli_args = ["doc", "-m", "test_model", "--config-path", str(config_filepath)]
    if test_and_flag_args:
        cli_args.append(test_and_flag_args)

    fp = FlagParser(cli_parser=parser)
    fp.consume_cli_arguments(test_cli_args=cli_args)

    config = DbtSugarConfig(fp)
    config.load_config()

    remapped = config._integrate_cli_flags(config.config_model.dict())
    assert remapped == expectation
