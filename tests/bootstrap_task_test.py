from pathlib import Path

import pytest

FIXTURE_DIR = Path(__file__).resolve().parent


@pytest.mark.datafiles(FIXTURE_DIR)
def test_build_all_models_dict(datafiles):
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

    task = BootstrapTask(
        flags=flag_parser, dbt_path=Path("tests/test_dbt_project"), sugar_config=config
    )
    models = task.build_all_models_dict()
    expectation = {
        "customers": {"path": Path("tests/test_dbt_project/jaffle_shop/models/customers.sql")},
        "orders": {"path": Path("tests/test_dbt_project/jaffle_shop/models/orders.sql")},
        "stg_customers": {
            "path": Path("tests/test_dbt_project/jaffle_shop/models/staging/stg_customers.sql")
        },
        "stg_payments": {
            "path": Path("tests/test_dbt_project/jaffle_shop/models/staging/stg_payments.sql")
        },
        "stg_orders": {
            "path": Path("tests/test_dbt_project/jaffle_shop/models/staging/stg_orders.sql")
        },
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
    assert models == expectation
