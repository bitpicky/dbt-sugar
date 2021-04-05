"""Holds config for dbt-sugar."""

from pathlib import Path
from typing import Any, Dict, List, Optional, Union

from pydantic import BaseModel

from dbt_sugar.core.clients.yaml_helpers import open_yaml
from dbt_sugar.core.exceptions import (
    KnownRegressionError,
    MissingDbtProjects,
    NoSyrupProvided,
    SyrupNotFoundError,
)
from dbt_sugar.core.flags import FlagParser
from dbt_sugar.core.logger import GLOBAL_LOGGER as logger


class DbtProjectsModel(BaseModel):
    """Pydantic validation model for dbt_project dict."""

    name: str
    path: str
    excluded_folders: Optional[Union[List[str], str]] = []
    excluded_models: Optional[Union[List[str], str]] = []


class SyrupModel(BaseModel):
    """Pydantic validation model for syrups dict."""

    name: str
    dbt_projects: List[DbtProjectsModel]
    always_enforce_tests: Optional[bool] = True
    always_add_tags: Optional[bool] = True


class DefaultsModel(BaseModel):
    """Pydantic validation model for defaults dict."""

    syrup: Optional[str]
    target: Optional[str]


class SugarConfigModel(BaseModel):
    """Pydantic validation model for sugar_config dict."""

    defaults: DefaultsModel
    syrups: List[SyrupModel]


class DbtSugarConfig:
    """dbt-sugar configuration class."""

    SUGAR_CONFIG_FILENAME = "sugar_config.yml"
    CLI_OVERRIDE_FLAGS = [
        {"cli_arg_name": "ask_for_tests", "maps_to": "always_enforce_tests"},
        {"cli_arg_name": "ask_for_tags", "maps_to": "always_add_tags"},
    ]

    def __init__(self, flags: FlagParser, max_dir_upwards_iterations: int = 4) -> None:
        """Constructor for DbtSugarConfig.

        Args:
            flags (FlagParser): consumed flags from FlagParser object.
        """
        self._flags = flags
        self._model_name: str = self._flags.model
        self._task = self._flags.task
        self._config_path = self._flags.config_path
        self._syrup_to_load = flags.syrup
        self._config_file_found_nearby = False
        self._max_folder_iterations = max_dir_upwards_iterations
        self._current_folder = Path.cwd()
        if self._flags.profiles_dir:
            self._current_folder = Path(self._flags.profiles_dir)

        # "externally offered objects"
        self.config_model: SyrupModel

    @property
    def config(self):
        if self.config_model:
            config_dict = self._integrate_cli_flags(self.config_model.dict())
            return config_dict
        raise AttributeError(f"{type(self).__name__} does not have a parsed config.")

    # ! REGRESSION
    @property
    def dbt_project_info(self):
        """Convenience function to ensure only one dbt project is unders scope

        This was introduced as part of an intentional regresssion because we're not ready
        to support multiple dbt projects yet.
        """
        return self.config.get("dbt_projects", list())[0]

    def load_and_validate_config_yaml(self) -> None:
        yaml_dict = open_yaml(self._config_path)

        # use pydantic to shape and validate
        self._config = SugarConfigModel(**yaml_dict)

    def parse_defaults(self) -> None:
        if self._config.defaults and not self._syrup_to_load:
            self._syrup_to_load = self._config.defaults.dict().get("syrup", str())

    def retain_syrup(self) -> None:
        if self._syrup_to_load:
            for syrup in self._config.syrups:
                syrup_dict = syrup.dict()
                if syrup_dict["name"] == self._syrup_to_load:
                    self.config_model = syrup

            if not hasattr(self, "config_model"):
                raise SyrupNotFoundError(
                    f"Could not find a syrup named {self._syrup_to_load} in {self._config_path}."
                )

        else:
            raise NoSyrupProvided(
                "A syrup must be provided either in your config.yml or passed to the CLI. "
                "Run `dbt-sugar --help` for more information."
            )

    # TODO: Deprecate this when we lift off and address this regression
    # ! REGRESSION
    def assert_only_one_dbt_project_in_scope(self) -> bool:
        number_of_dbt_projects = len(self.config.get("dbt_projects", list()))
        if number_of_dbt_projects > 1:
            raise KnownRegressionError(
                "dbt-sugar can only support ONE dbt project per sugar. "
                "This limitation will be lifted in the next feature release."
            )
        return True

    def assert_dbt_projects_exist(self) -> bool:
        dbt_projects = self.config["dbt_projects"]

        project_existance = {}
        for project in dbt_projects:
            resolved_dbt_project_path = Path(project["path"]).resolve()
            logger.debug(f"Looking for {project['name']} in {resolved_dbt_project_path}")
            project_existance[project["name"]] = {
                "exists": True if resolved_dbt_project_path.exists() else False,
                "path": resolved_dbt_project_path,
            }

        bogus_projects = dict()
        for project, details in project_existance.items():
            if details["exists"] is False:
                bogus_projects[project] = details["path"]

        # TODO: Maybe we want to revisit this and not have a raise but rather a logger warning and says we'll ignore
        if bogus_projects:
            raise MissingDbtProjects(
                f"The following dbt projects are missing or cannot be found: \n\n{bogus_projects}. \n\n"
                "Check your sugar_config.yml"
            )
        return True

    def locate_config(self) -> None:
        folder_iteration = 0
        logger.debug(f"Starting config file finding from {self._current_folder}")
        current = self._current_folder
        filename = Path(current).joinpath(self.SUGAR_CONFIG_FILENAME)

        if self._config_path == Path(str()):
            logger.debug("Trying to find sugar_config.yml in current and parent folders")

            while folder_iteration < self._max_folder_iterations:
                if filename.exists():
                    sugar_config_dir = filename
                    logger.debug(f"{filename} exists and was retreived.")
                    self._config_path = sugar_config_dir
                    self._config_file_found_nearby = True
                    break
                current = current.parent
                filename = Path(current, self.SUGAR_CONFIG_FILENAME)
                folder_iteration += 1

            else:
                raise FileNotFoundError(
                    f"Unable to find {self.SUGAR_CONFIG_FILENAME} in any nearby "
                    f"directories after {self._max_folder_iterations} iterations upwards."
                )

    def _integrate_cli_flags(self, config_dict: Dict[str, Any]):
        for flag_override_dict in self.CLI_OVERRIDE_FLAGS:
            config_dict[flag_override_dict["maps_to"]] = getattr(
                self._flags, flag_override_dict["cli_arg_name"]
            )
        return config_dict

    def load_config(self) -> None:
        self.locate_config()
        self.load_and_validate_config_yaml()
        self.parse_defaults()
        self.retain_syrup()
        self.assert_only_one_dbt_project_in_scope()
        _ = self.assert_dbt_projects_exist()
        logger.debug(f"Config model dict: {self.config_model.dict()}")
