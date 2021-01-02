"""Holds config for dbt-sugar."""

from typing import List, Optional, Union

from pydantic import BaseModel

from dbt_sugar.core.clients.yaml_helpers import open_yaml
from dbt_sugar.core.exceptions import NoSugarCaneProvided, SugarCaneNotFoundError
from dbt_sugar.core.flags import FlagParser


class DbtProjectsModel(BaseModel):
    """Pydantic validation model for dbt_project dict."""

    name: str
    path: str
    excluded_tables: Optional[Union[List[str], str]]


class SugarCanesModel(BaseModel):
    """Pydantic validation model for sugar_canes dict."""

    name: str
    dbt_projects: List[DbtProjectsModel]


class DefaultsModel(BaseModel):
    """Pydantic validation model for defaults dict."""

    sugar_cane: Optional[str]
    target: Optional[str]


class SugarConfigModel(BaseModel):
    """Pydantic validation model for sugar_config dict."""

    defaults: DefaultsModel
    sugar_canes: List[SugarCanesModel]


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
        self._cane_to_load = flags.sugar_cane

        # "externally offered objects"
        self.config_model: SugarCanesModel

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
        if self._config.defaults and not self._cane_to_load:
            self._cane_to_load = self._config.defaults.dict().get("sugar_cane", str())

    def retain_cane(self) -> None:
        if self._cane_to_load:
            for cane in self._config.sugar_canes:
                cane_dict = cane.dict()
                if cane_dict["name"] == self._cane_to_load:
                    self.config_model = cane

            if not hasattr(self, "config_model"):
                raise SugarCaneNotFoundError(
                    f"Could not find a sugar cane named {self._cane_to_load} in {self._config_path}."
                )

        else:
            raise NoSugarCaneProvided(
                "A sugar cane must be provided either in your config.yml or passed to the CLI. "
                "Run `dbt-sugar --help` for more information."
            )

    def load_config(self) -> None:
        self.load_and_validate_config_yaml()
        self.parse_defaults()
        self.retain_cane()
