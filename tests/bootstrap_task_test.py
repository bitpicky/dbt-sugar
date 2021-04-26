from pathlib import Path

import pytest

FIXTURE_DIR = Path(__file__).resolve().parent


@pytest.mark.datafiles(FIXTURE_DIR)
def test_build_all_models_dict(datafiles):
    from dbt_sugar.core.clients.dbt import DbtProfile
    from dbt_sugar.core.config.config import DbtSugarConfig
    from dbt_sugar.core.flags import FlagParser
    from dbt_sugar.core.main import parser
    from dbt_sugar.core.task.bootstrap import BootstrapTask

    config_filepath = Path(datafiles).joinpath("sugar_config.yml")
    flag_parser = FlagParser(parser)
    cli_args = ["bootstrap", "--config-path", str(config_filepath)]
    flag_parser.consume_cli_arguments(test_cli_args=cli_args)
    config = DbtSugarConfig(flag_parser)
    config.load_config()

    profile = DbtProfile(
        flags=flag_parser,
        profile_name="dbt_sugar_test",
        target_name=str(),
        profiles_dir=Path(datafiles),
    )
    profile.read_profile()

    task = BootstrapTask(
        flags=flag_parser,
        dbt_path=Path("tests/test_dbt_project/dbt_sugar_test"),
        sugar_config=config,
        dbt_profile=profile,
    )
    task.build_all_models_dict()
    expectation = {
        "my_first_dbt_model": {
            "path": Path(
                "tests/test_dbt_project/dbt_sugar_test/models/example/my_first_dbt_model.sql"
            )
        },
        "my_second_dbt_model": {
            "path": Path(
                "tests/test_dbt_project/dbt_sugar_test/models/example/my_second_dbt_model.sql"
            )
        },
    }
    assert task.dbt_models_dict == expectation


# @pytest.mark.datafiles(FIXTURE_DIR)
# def test_check_colums_in_db(datafiles):
# from dbt_sugar.core.clients.dbt import DbtProfile
# from dbt_sugar.core.config.config import DbtSugarConfig
# from dbt_sugar.core.flags import FlagParser
# from dbt_sugar.core.main import parser
# from dbt_sugar.core.task.bootstrap import BootstrapTask

# config_filepath = Path(datafiles).joinpath("sugar_config.yml")
# flag_parser = FlagParser(parser)
# cli_args = ["bootstrap", "--config-path", str(config_filepath)]
# flag_parser.consume_cli_arguments(test_cli_args=cli_args)
# config = DbtSugarConfig(flag_parser)
# config.load_config()

# profile = DbtProfile(
# flags=flag_parser,
# profile_name="dbt_sugar_test",
# target_name=str(),
# profiles_dir=Path(datafiles),
# )
# profile.read_profile()

# task = BootstrapTask(
# flags=flag_parser,
# dbt_path=Path("tests/test_dbt_project"),
# sugar_config=config,
# dbt_profile=profile,
# )
# task.dbt_models_dict = {
# "my_first_dbt_model": {
# "path": Path(
# "tests/test_dbt_project/dbt_sugar_test/models/example/my_first_dbt_model.sql"
# )
# },
# "my_second_dbt_model": {
# "path": Path(
# "tests/test_dbt_project/dbt_sugar_test/models/example/my_second_dbt_model.sql"
# )
# },
# }
# expectation = {
# "my_first_dbt_model": {
# "path": Path(
# "tests/test_dbt_project/dbt_sugar_test/models/example/my_first_dbt_model.sql"
# ),
# "columns": ["id", "answer", "question"],
# },
# "my_second_dbt_model": {
# "path": Path(
# "tests/test_dbt_project/dbt_sugar_test/models/example/my_second_dbt_model.sql"
# ),
# "columns": ["id", "answer", "question"],
# },
# }
# task.check_colums_in_db()
# assert task.dbt_models_dict == expectation


@pytest.mark.datafiles(FIXTURE_DIR)
def test_get_columns_and_descriptor_path(datafiles):
    from dbt_sugar.core.clients.dbt import DbtProfile
    from dbt_sugar.core.config.config import DbtSugarConfig
    from dbt_sugar.core.flags import FlagParser
    from dbt_sugar.core.main import parser
    from dbt_sugar.core.task.bootstrap import BootstrapTask

    config_filepath = Path(datafiles).joinpath("sugar_config.yml")
    flag_parser = FlagParser(parser)
    cli_args = ["bootstrap", "--config-path", str(config_filepath)]
    flag_parser.consume_cli_arguments(test_cli_args=cli_args)
    config = DbtSugarConfig(flag_parser)
    config.load_config()

    profile = DbtProfile(
        flags=flag_parser,
        profile_name="dbt_sugar_test",
        target_name=str(),
        profiles_dir=Path(datafiles),
    )
    profile.read_profile()

    task = BootstrapTask(
        flags=flag_parser,
        dbt_path=Path("tests/test_dbt_project"),
        sugar_config=config,
        dbt_profile=profile,
    )
    task.dbt_models_dict = {"my_first_dbt_model": {}, "my_second_dbt_model": {}}
    task.get_columns_and_descriptor_path()
    expectation = {
        "my_first_dbt_model": {
            "model_descriptor_path": (
                Path("tests/test_dbt_project/dbt_sugar_test/models/example/schema.yml"),
                False,
                False,
            )
        },
        "my_second_dbt_model": {
            "model_descriptor_path": (
                Path("tests/test_dbt_project/dbt_sugar_test/models/example/arbitrary_name.yml"),
                True,
                True,
            )
        },
    }
    assert task.dbt_models_dict == expectation
