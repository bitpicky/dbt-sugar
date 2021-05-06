from collections import OrderedDict
from pathlib import Path

import pytest

FIXTURE_DIR = Path(__file__).resolve().parent


@pytest.mark.datafiles(FIXTURE_DIR)
def test_build_all_models_dict(datafiles):
    from dbt_sugar.core.clients.dbt import DbtProfile
    from dbt_sugar.core.config.config import DbtSugarConfig
    from dbt_sugar.core.flags import FlagParser
    from dbt_sugar.core.main import parser
    from dbt_sugar.core.task.bootstrap import BootstrapTask, DbtModelsDict

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
    print("BUILT")
    print(task.dbt_models_data)
    expectation = [
        DbtModelsDict(
            model_name="my_first_dbt_model",
            model_path=Path(
                "tests/test_dbt_project/dbt_sugar_test/models/example/my_first_dbt_model.sql"
            ),
            model_columns=[],
        ),
        DbtModelsDict(
            model_name="my_second_dbt_model",
            model_path=Path(
                "tests/test_dbt_project/dbt_sugar_test/models/example/my_second_dbt_model.sql"
            ),
            model_columns=[],
        ),
    ]
    print("EXPT")
    print(expectation)
    assert task.dbt_models_data == expectation


@pytest.mark.datafiles(FIXTURE_DIR)
def test_add_or_update_model_descriptor_placeholders(datafiles):
    from dbt_sugar.core.clients.dbt import DbtProfile
    from dbt_sugar.core.config.config import DbtSugarConfig
    from dbt_sugar.core.flags import FlagParser
    from dbt_sugar.core.main import parser
    from dbt_sugar.core.task.bootstrap import BootstrapTask, DbtModelsDict

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
    task.dbt_models_data = [
        DbtModelsDict(model_name="my_first_dbt_model", model_path=None, model_columns=[])
    ]
    rez = task.add_or_update_model_descriptor_placeholders(is_test=True)
    expectation = OrderedDict(
        [
            ("version", 2),
            (
                "models",
                [
                    OrderedDict(
                        [
                            ("name", "my_first_dbt_model"),
                            ("description", "No description for this model."),
                            (
                                "columns",
                                [
                                    OrderedDict(
                                        [
                                            ("name", "answer"),
                                            ("description", "No description for this column."),
                                        ]
                                    ),
                                    OrderedDict(
                                        [
                                            ("name", "id"),
                                            ("description", "No description for this column."),
                                        ]
                                    ),
                                    OrderedDict(
                                        [
                                            ("name", "question"),
                                            ("description", "No description for this column."),
                                        ]
                                    ),
                                ],
                            ),
                        ]
                    )
                ],
            ),
        ]
    )
    assert rez == expectation
