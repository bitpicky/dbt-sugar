import pytest


@pytest.mark.parametrize(
    "cli_args",
    [["doc", "-m", "test_model"], ["doc", "-m", "test_model", "--log-level", "debug"]],
)
def test_handle(cli_args):
    from dbt_sugar.core.main import handle, parser

    handle_result = handle(parser, test_cli_args=cli_args)

    assert handle_result == 0
