from pathlib import Path

import pytest
from pydantic.error_wrappers import ValidationError

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
            "account": None,
            "database": "dbt_sugar",
            "password": "magical_password",
            "role": None,
            "target_schema": "public",
            "type": "postgres",
            "user": "dbt_sugar_test_user",
            "warehouse": None,
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
                project_name="dbt_sugar_test_project",
                target_name=target_name,
                profiles_dir=Path(datafiles).joinpath("profiles.yml"),
            )
            profile.read_profile()
    elif is_invalid_target:
        with pytest.raises(ProfileParsingError):
            profile = DbtProfile(
                project_name="dbt_sugar_test_project",
                target_name=target_name,
                profiles_dir=Path(datafiles).joinpath("profiles.yml"),
            )
            profile.read_profile()
    elif is_missing_profile:
        with pytest.raises(DbtProfileFileMissing):
            profile = DbtProfile(
                project_name="dbt_sugar_test_project",
                target_name=target_name,
                profiles_dir=Path(datafiles).joinpath("missing_profiles.yml"),
            )
            profile.read_profile()
    elif is_bad_project:
        with pytest.raises(ProfileParsingError):
            profile = DbtProfile(
                project_name="bad_project",
                target_name=target_name,
                profiles_dir=Path(datafiles).joinpath("profiles.yml"),
            )
            profile.read_profile()
    else:
        profile = DbtProfile(
            project_name="dbt_sugar_test_project",
            target_name=target_name,
            profiles_dir=Path(datafiles).joinpath("profiles.yml"),
        )
        profile.read_profile()
        assert profile.profile == expectations[target_name]
