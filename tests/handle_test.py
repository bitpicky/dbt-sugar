from pathlib import Path

import pytest

TEST_PROFILES_DIR = Path(__file__).resolve().parent


@pytest.mark.parametrize(
    "cli_args",
    [
        ["doc", "-m", "test_model", "--profiles-dir", str(TEST_PROFILES_DIR), "--dry-run"],
        [
            "doc",
            "-m",
            "test_model",
            "--log-level",
            "debug",
            "--profiles-dir",
            str(TEST_PROFILES_DIR),
            "--dry-run",
        ],
    ],
)
def test_handle(cli_args):
    from dbt_sugar.core.main import handle, parser

    handle_result = handle(parser, test_cli_args=cli_args)

    assert handle_result == 0
