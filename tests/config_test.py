import pytest


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
