from pathlib import Path

import pytest

FIXTURE_DIR = Path(__file__).resolve().parent


@pytest.mark.datafiles(FIXTURE_DIR)
def test_read_project(datafiles):
    from dbt_sugar.core.clients.dbt import DbtProject
    from dbt_sugar.core.config.config import DbtSugarConfig
    from dbt_sugar.core.flags import FlagParser
    from dbt_sugar.core.main import parser

    config_filepath = Path(datafiles).joinpath("sugar_config.yml")
    cli_args = ["doc", "--config-path", str(config_filepath), "--syrup", "syrup_1"]

    flag_parser = FlagParser(parser)
    flag_parser.consume_cli_arguments(test_cli_args=cli_args)

    sugar_config = DbtSugarConfig(flag_parser)
    sugar_config.load_config()
    print(f"the config {sugar_config.config}")
    print(sugar_config.config.get("path"))

    dbt_project = DbtProject(
        project_name=sugar_config.config.get("dbt_projects", list())[0].get("name", str()),
        project_dir=sugar_config.config.get("dbt_projects", list())[0].get("path", str()),
    )
    dbt_project.read_project()

    assert dbt_project.profile_name == "dbt_sugar_test"
