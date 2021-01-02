"""Holds config for dbt-sugar."""

from pathlib import Path
from typing import List, Optional, Union

from pydantic import BaseModel

from dbt_sugar.core.clients.yaml_helpers import open_yaml
from dbt_sugar.core.exceptions import MissingDbtProjects, NoSyrupProvided, SyrupNotFoundError
from dbt_sugar.core.flags import FlagParser


class DbtProjectsModel(BaseModel):
    """Pydantic validation model for dbt_project dict."""

    name: str
    path: str
    excluded_tables: Optional[Union[List[str], str]]


class SyrupModel(BaseModel):
    """Pydantic validation model for syrups dict."""

    name: str
    dbt_projects: List[DbtProjectsModel]


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

    def __init__(self, flags: FlagParser) -> None:
        """Constructor for DbtSugarConfig.

        Args:
            flags (FlagParser): consumed flags from FlagParser object.
        """
        self._flags = flags
        self._model_name: str = self._flags.model
        self._task = self._flags.task
        self._config_path = self._flags.config_path
        self._syrup_to_load = flags.syrup

        # "externally offered objects"
        self.config_model: SyrupModel

    @property
    def config(self):
        if self.config_model:
            return self.config_model.dict()
        raise AttributeError(f"{type(self).__name__} does not have a parsed config.")

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

    def assert_dbt_projects_exist(self) -> bool:
        dbt_projects = self.config["dbt_projects"]

        project_existance = {}
        for project in dbt_projects:
            project_existance[project["name"]] = (
                True if Path(project["path"]).resolve().exists() else False
            )

        bogus_projects = dict()
        for project, exists in project_existance.items():
            if exists is False:
                bogus_projects[project] = exists

        # TODO: Maybe we want to revisit this and not have a raise but rather a logger warning and says we'll ignore
        if bogus_projects:
            raise MissingDbtProjects(
                f"The following dbt projects are missing: \n{list(bogus_projects.keys())}"
            )
        return True

    def load_config(self) -> None:
        self.load_and_validate_config_yaml()
        self.parse_defaults()
        self.retain_syrup()
        _ = self.assert_dbt_projects_exist()
