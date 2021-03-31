"""Holds methods to interact with dbt API (we mostly don't for now because not stable) and objects."""

from pathlib import Path
from typing import Any, Dict, Optional, Union

from pydantic import BaseModel, Field

from dbt_sugar.core.clients.yaml_helpers import open_yaml
from dbt_sugar.core.exceptions import (
    DbtProfileFileMissing,
    ProfileParsingError,
    TargetNameNotProvided,
)
from dbt_sugar.core.flags import FlagParser
from dbt_sugar.core.logger import GLOBAL_LOGGER as logger

DEFAULT_DBT_PROFILE_PATH = Path.home().joinpath(".dbt")


class PostgresDbtProfilesModel(BaseModel):
    """Postgres Dbt credentials validation model."""

    type: str
    user: str
    password: str = Field(..., alias="pass")
    database: str = Field(..., alias="dbname")
    target_schema: str = Field(..., alias="schema")
    host: str
    port: int


class SnowflakeDbtProfilesModel(BaseModel):
    """Snowflake Dbt credentials validation model."""

    type: str
    account: str
    user: str
    password: str
    database: str
    target_schema: str = Field(..., alias="schema")
    role: str
    warehouse: str


class DbtProjectModel(BaseModel):
    """Defines pydandic validation schema for a dbt_project.yml file."""

    profile: str


class BaseYamlConfig:
    """Base class object which gets extended by objects which will generally read from yaml configs."""

    def _assert_file_exists(self, dir: Path, filename: str = "profiles.yml") -> bool:
        logger.debug(dir.resolve())
        full_path_to_file = dir / filename
        if full_path_to_file.is_file():
            return True
        else:
            raise DbtProfileFileMissing(f"Could not locate `{filename}` in {dir.resolve()}")


class DbtProject(BaseYamlConfig):
    """Holds parsed dbt project information needed for dbt-sugar such as which db profile to target."""

    DBT_PROJECT_FILENAME: str = "dbt_project.yml"

    def __init__(self, project_name: str, project_dir: Path) -> None:
        """Constructor for DbtProject.

        Given a project name and a project dir it will parse the relevant dbt_project.yml and
        parse information such as `profile` so dbt-sugar knows which database profile entry from
        /.dbt/profiles.yml to use.

        Args:
            project_name (str): Name of the dbt project to read profile from.
            project_dir (Path): Path object the dbt_project.yml to read from.
        """
        self._project_name = project_name
        self._project_dir = project_dir

        # class "outputs"
        self.project: DbtProjectModel
        self.profile_name: str

    @property
    def _dbt_project_filename(self) -> Path:
        logger.debug(f"project_dir: {self._project_dir}")
        return Path(self._project_dir).joinpath(type(self).DBT_PROJECT_FILENAME)

    def read_project(self) -> None:
        _ = self._assert_file_exists(Path(self._project_dir), filename=self.DBT_PROJECT_FILENAME)
        _project_dict = open_yaml(self._dbt_project_filename)

        # pass the dict through pydantic for validation and only getting what we need
        # if the profile is invalid app will crash so no further tests required below.
        logger.debug(f"the project {_project_dict}")
        _project = DbtProjectModel(**_project_dict)
        logger.debug(_project)
        self.project = _project
        self.profile_name = self.project.dict().get("profile", str())

        if not self.profile_name:
            logger.warning(
                f"[yellow]There was no `profile:` entry in {self._dbt_project_filename}. "
                "dbt-sugar will try to find a 'default' profile. This might lead to unexpected"
                "behaviour or an error when no defaulf profile can be found in your dbt profiles.yml"
            )


class DbtProfile(BaseYamlConfig):
    """Holds parsed profile dict from dbt profiles."""

    CLI_OVERRIDE_FLAGS = [{"cli_arg_name": "schema", "maps_to": "target_schema"}]

    def __init__(
        self,
        flags: FlagParser,
        profile_name: str,
        target_name: str,
        profiles_dir: Optional[Path] = None,
    ) -> None:
        """Reads, validates and holds dbt profile info required by dbt-sugar (mainly db creds).

        Args:
            project_name (str): name of the dbt project to read credentials from.
            target_name (str): name of the target entry. This corresponds to what resides below
                "outputs" in the dbt's profile.yml (https://docs.getdbt.com/dbt-cli/configure-your-profile/)
        """
        # attrs parsed from constructor
        self._flags = flags
        self._profile_name = profile_name
        self._target_name = target_name
        self._profiles_dir = profiles_dir

        # attrs populated by class methods
        self.profile: Dict[str, str]

    @property
    def profiles_dir(self):
        if self._profiles_dir:
            return self._profiles_dir
        return DEFAULT_DBT_PROFILE_PATH

    def _get_target_profile(self, profile_dict: Dict[str, Any]) -> Dict[str, Union[str, int]]:
        if self._target_name:
            return profile_dict["outputs"].get(self._target_name)
        else:
            self._target_name = profile_dict.get("target", str())
            if self._target_name:
                return profile_dict["outputs"].get(self._target_name)
            else:
                raise TargetNameNotProvided(
                    f"No target name provied in {self._profiles_dir} and none provided via "
                    "--target in CLI. Cannot figure out appropriate profile information to load."
                )

    def read_profile(self):
        _ = self._assert_file_exists(
            self.profiles_dir
        )  # this will raise so no need to check exists further
        _profile_dict = open_yaml(self.profiles_dir / "profiles.yml")
        _profile_dict = _profile_dict.get(self._profile_name, _profile_dict.get(self._profile_name))
        if _profile_dict:

            # read target name from args or try to get it from the dbt_profile `target:` field.
            _target_profile = self._get_target_profile(profile_dict=_profile_dict)

            if _target_profile:
                _profile_type = _target_profile.get("type")
                # call the right pydantic validator depending on the db type as dbt is not
                # consistent with it's profiles and it's hell to have all the validation in one
                # pydantic model.
                if _profile_type == "snowflake":
                    # uses pydantic to validate profile. It will raise and break app if invalid.
                    _target_profile = SnowflakeDbtProfilesModel(**_target_profile)
                elif _profile_type == "postgres":
                    _target_profile = PostgresDbtProfilesModel(**_target_profile)

                # if we don't manage to read the db type for some reason.
                elif _profile_type is None:
                    raise ProfileParsingError(
                        f"Could not read or find a database type for {self._profile_name} in your dbt "
                        "profiles.yml. Check that this field is not missing."
                    )
                else:
                    raise NotImplementedError(f"{_profile_type} is not implemented yet.")
                logger.debug(_target_profile)
                self.profile = _target_profile.dict()

                # override profile info with potential CLI args
                self._integrate_cli_flags()

            else:
                raise ProfileParsingError(
                    f"Could not find an entry for target: '{self._target_name}', "
                    f"for the '{self._profile_name}' profile in your dbt profiles.yml."
                )

        else:
            raise ProfileParsingError(
                f"Could not find an entry for '{self._profile_name}' in your profiles.yml"
            )

    def _integrate_cli_flags(self) -> None:
        for flag_override_dict in self.CLI_OVERRIDE_FLAGS:
            cli_arg_value = getattr(self._flags, flag_override_dict["cli_arg_name"])
            if cli_arg_value and isinstance(self.profile, dict):
                self.profile[flag_override_dict["maps_to"]] = cli_arg_value
            else:
                logger.debug("No schema passed to CLI will try to read from profile.yml")
