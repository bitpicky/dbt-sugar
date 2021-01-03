"""Holds methods to interact with dbt API (we mostly don't for now because not stable) and objects."""

from pathlib import Path
from typing import Dict, Optional

from pydantic import BaseModel, root_validator

from dbt_sugar.core.clients.yaml_helpers import open_yaml
from dbt_sugar.core.exceptions import DbtProfileFileMissing, ProfileParsingError
from dbt_sugar.core.logger import GLOBAL_LOGGER as logger

DEFAULT_DBT_PROFILE_PATH = Path.home().joinpath(".dbt", "profiles").with_suffix(".yml")


class DbtProfilesModel(BaseModel):
    """Dbt credentials validation model."""

    type: str
    account: Optional[str]
    user: str
    password: str
    database: str
    target_schema: str
    role: Optional[str] = None
    warehouse: Optional[str] = None

    @root_validator(pre=True)
    def check_required_fields_based_on_db_type(cls, values):
        """Checks some fields are not None based on different db requirements."""
        if values.get("type") == "snowflake":
            snowflake_fields = ["account", "role", "warehouse"]
            for field in snowflake_fields:
                assert values.get(field) is not None
        return values

    class Config:
        """Handles fields renaming.

        Mainly used to remap words that are reserved by pydantic.
        """

        fields = {"target_schema": "schema"}


class DbtProfile:
    """Holds parsed profile dict from dbt profiles."""

    def __init__(
        self, project_name: str, target_name: str, profiles_dir: Optional[Path] = None
    ) -> None:
        """Reads, validates and holds dbt profile info required by dbt-sugar (mainly db creds).

        Args:
            project_name (str): name of the dbt project to read credentials from.
            target_name (str): name of the target entry. This corresponds to what resides below
                "outputs" in the dbt's profile.yml (https://docs.getdbt.com/dbt-cli/configure-your-profile/)
        """
        # attrs parsed from constructor
        self.project_name = project_name
        # TODO: dbt profile allows for a default target to be specified. We might want to allow
        # for this to be null and parse the target from "target:" key.
        self.target_name = target_name
        self._profiles_dir = profiles_dir

        # attrs populated by class methods
        self.profile: Optional[Dict[str, str]] = None

    @property
    def profiles_dir(self):
        if self._profiles_dir:
            return self._profiles_dir
        return DEFAULT_DBT_PROFILE_PATH

    def _assert_file_exists(self) -> bool:
        # TODO: We'll want to allow users to override this path.
        if self.profiles_dir.is_file():
            return True
        else:
            raise DbtProfileFileMissing(
                f"Could not locate `profiles.yml` in {self.profiles_dir.resolve()}."
            )

    def read_profile(self):
        _ = self._assert_file_exists()  # this will raise so no need to check exists further
        _profile_dict = open_yaml(self.profiles_dir)
        _profile_dict = _profile_dict.get(self.project_name, _profile_dict.get("default"))
        if _profile_dict:
            _target_profile = _profile_dict["outputs"].get(self.target_name)

            if _target_profile:
                # uses pydantic to validate profile. It will raise and break app if invalid.
                _target_profile = DbtProfilesModel(**_target_profile)
                logger.debug(_target_profile)
                self.profile = _target_profile.dict()
            else:
                raise ProfileParsingError(
                    f"Could not find an entry for target: {self.target_name}, "
                    f"under the {self.project_name} config."
                )

        else:
            raise ProfileParsingError(
                f"Could not find an entry for {self.project_name} in your profiles.yml "
                f"located in {self.profiles_dir}."
            )
