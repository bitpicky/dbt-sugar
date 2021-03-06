from pathlib import Path

import pytest
from pydantic.error_wrappers import ValidationError

from dbt_sugar.core.clients.dbt import DbtProfile

FIXTURE_DIR = Path(__file__).resolve().parent


@pytest.mark.parametrize(
    "target_name, is_missing_profile, is_invalid_target, is_bad_project",
    [
        ("snowflake", False, False, False),
        ("postgres", False, False, False),
        ("bad_snowflake", False, False, False),
        ("bad_postgres", False, False, False),
        ("_postgres", False, True, False),
        ("missing_profile", True, False, False),
        ("_postgres", False, True, True),
    ],
)
@pytest.mark.datafiles(FIXTURE_DIR)
def test_read_profile(
    datafiles, target_name, is_missing_profile, is_invalid_target, is_bad_project
):
    from dbt_sugar.core.clients.dbt import DbtProfile
    from dbt_sugar.core.exceptions import DbtProfileFileMissing, ProfileParsingError
    from dbt_sugar.core.flags import FlagParser
    from dbt_sugar.core.main import parser

    cli_args = ["doc", "-m", "dummy_model"]
    flag_parser = FlagParser(parser)
    flag_parser.consume_cli_arguments(test_cli_args=cli_args)

    expectations = {
        "snowflake": {
            "type": "snowflake",
            "account": "dummy_account",
            "user": "dummy_user",
            "password": "dummy_password",
            "database": "dummy_database",
            "target_schema": "dummy_target_schema",
            "role": "dummy_role",
            "warehouse": "dummy_warehouse",
        },
        "postgres": {
            "database": "dbt_sugar",
            "password": "magical_password",
            "target_schema": "public",
            "type": "postgres",
            "user": "dbt_sugar_test_user",
            "host": "localhost",
            "port": 5432,
        },
        "bad_snowflake": {
            "type": "snowflake",
            "user": "dummy_user",
            "database": "dummy_database",
            "target_schema": "dummy_target_schema",
            "role": "dummy_role",
            "warehouse": "dummy_warehouse",
        },
        "bad_postgres": {
            "database": "dbt_sugar",
            "type": "postgres",
            "user": "dbt_sugar_test_user",
            "target_schema": "public",
        },
    }

    if target_name.startswith("bad_"):
        with pytest.raises(ValidationError):
            profile = DbtProfile(
                flags=flag_parser,
                profile_name="dbt_sugar_test",
                target_name=target_name,
                profiles_dir=Path(datafiles),
            )
            profile.read_profile()

    elif is_invalid_target:
        with pytest.raises(ProfileParsingError):
            profile = DbtProfile(
                flags=flag_parser,
                profile_name="dbt_sugar_test",
                target_name=target_name,
                profiles_dir=Path(datafiles),
            )
            profile.read_profile()

    elif is_missing_profile:
        with pytest.raises(DbtProfileFileMissing):
            profile = DbtProfile(
                flags=flag_parser,
                profile_name="dbt_sugar_test",
                target_name=target_name,
                profiles_dir=Path(datafiles).joinpath("missing_profiles.yml"),
            )
            profile.read_profile()

    elif is_bad_project:
        with pytest.raises(ProfileParsingError):
            profile = DbtProfile(
                flags=flag_parser,
                profile_name="bad_project",
                target_name=target_name,
                profiles_dir=Path(datafiles),
            )
            profile.read_profile()

    else:
        profile = DbtProfile(
            flags=flag_parser,
            profile_name="dbt_sugar_test",
            target_name=target_name,
            profiles_dir=Path(datafiles),
        )
        profile.read_profile()
        assert profile.profile == expectations[target_name]

        # this one tests the auto parsing of the "target:" field in profiles.yml
        if target_name == "postgres":
            profile = DbtProfile(
                flags=flag_parser,
                profile_name="dbt_sugar_test",
                target_name=str(),
                profiles_dir=Path(datafiles),
            )
            profile.read_profile()
            assert profile.profile == expectations[target_name]


@pytest.mark.datafiles(FIXTURE_DIR)
def test_read_profile_missing(datafiles):
    from dbt_sugar.core.clients.dbt import DbtProfile
    from dbt_sugar.core.exceptions import ProfileParsingError
    from dbt_sugar.core.flags import FlagParser
    from dbt_sugar.core.main import parser

    cli_args = ["doc", "-m", "dummy_model"]
    flag_parser = FlagParser(parser)
    flag_parser.consume_cli_arguments(test_cli_args=cli_args)
    with pytest.raises(ProfileParsingError):
        profile = DbtProfile(
            flags=flag_parser,
            profile_name="tough shit it does not exist",
            target_name=str(),
            profiles_dir=Path(datafiles),
        )
        profile.read_profile()


@pytest.mark.parametrize(
    "profile_dict",
    [
        {
            "outputs": {
                "postgres": {
                    "type": "postgres",
                    "user": "dbt_sugar_test_user",
                    "password": "magical_password",
                    "database": "dbt_sugar",
                    "schema": "public",
                },
            },
            "target": "postgres",
        }
    ],
)
@pytest.mark.datafiles(FIXTURE_DIR)
def test_get_target_profile(datafiles, profile_dict):
    from dbt_sugar.core.clients.dbt import DbtProfile
    from dbt_sugar.core.flags import FlagParser
    from dbt_sugar.core.main import parser

    cli_args = ["doc", "-m", "dummy_model"]
    flag_parser = FlagParser(parser)
    flag_parser.consume_cli_arguments(test_cli_args=cli_args)

    profile = DbtProfile(
        flags=flag_parser,
        profile_name="dbt_sugar_test",
        target_name=str(),
        profiles_dir=Path(datafiles).joinpath("profiles.yml"),
    )
    target_profile = profile._get_target_profile(profile_dict=profile_dict)

    assert target_profile == profile_dict["outputs"]["postgres"]


@pytest.mark.parametrize(
    "profile_dict",
    [
        {
            "outputs": {
                "postgres": {
                    "type": "postgres",
                    "user": "dbt_sugar_test_user",
                    "password": "magical_password",
                    "database": "dbt_sugar",
                    "schema": "public",
                },
            },
        }
    ],
)
@pytest.mark.datafiles(FIXTURE_DIR)
def test_get_target_profile_no_target(datafiles, profile_dict):
    from dbt_sugar.core.clients.dbt import DbtProfile
    from dbt_sugar.core.exceptions import TargetNameNotProvided
    from dbt_sugar.core.flags import FlagParser
    from dbt_sugar.core.main import parser

    cli_args = ["doc", "-m", "dummy_model"]
    flag_parser = FlagParser(parser)
    flag_parser.consume_cli_arguments(test_cli_args=cli_args)
    profile = DbtProfile(
        flags=flag_parser,
        profile_name="dbt_sugar_test",
        target_name=str(),
        profiles_dir=Path(datafiles).joinpath("profiles.yml"),
    )
    with pytest.raises(TargetNameNotProvided):
        _ = profile._get_target_profile(profile_dict=profile_dict)


@pytest.mark.parametrize(
    "cli_args, expectation",
    [
        pytest.param(
            ["doc", "-m", "dummy_model", "--schema", "cli_passed_schema"],
            {
                "type": "postgres",
                "user": "dbt_sugar_test_user",
                "password": "magical_password",
                "database": "dbt_sugar",
                "target_schema": "cli_passed_schema",
                "host": "localhost",
                "port": 5432,
            },
            id="cli_overriden_schema",
        ),
        pytest.param(
            ["doc", "-m", "dummy_model"],
            {
                "type": "postgres",
                "user": "dbt_sugar_test_user",
                "password": "magical_password",
                "database": "dbt_sugar",
                "target_schema": "public",
                "host": "localhost",
                "port": 5432,
            },
            id="schema_from_config",
        ),
    ],
)
@pytest.mark.datafiles(FIXTURE_DIR)
def test__integrate_cli_flags(datafiles, cli_args, expectation):
    from dbt_sugar.core.flags import FlagParser
    from dbt_sugar.core.main import parser

    flag_parser = FlagParser(parser)
    flag_parser.consume_cli_arguments(test_cli_args=cli_args)
    profile = DbtProfile(
        flags=flag_parser,
        profile_name="dbt_sugar_test",
        target_name="postgres",
        profiles_dir=Path(datafiles),
    )
    profile.read_profile()
    profile._integrate_cli_flags()
    assert profile.profile == expectation
