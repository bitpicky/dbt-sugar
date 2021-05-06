"""Bootstrap module. Generates placeholders for all models in a dbt project."""


import functools
import operator
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Sequence, Union

from dbt_sugar.core.clients.dbt import DbtProfile
from dbt_sugar.core.clients.yaml_helpers import open_yaml, save_yaml
from dbt_sugar.core.config.config import DbtSugarConfig
from dbt_sugar.core.flags import FlagParser
from dbt_sugar.core.task.doc import DocumentationTask


@dataclass
class DbtModelsDict:
    """Data class for dbt model info.

    We make it a dataclass instead of a dict because the types
    inside a dict would have been too messy and was upstetting type checkers.
    """

    model_name: str
    model_path: Path
    model_columns: Sequence[str]


class BootstrapTask(DocumentationTask):
    """Sets up methods and orchestration of the bootstrap task.

    The bootstrap task is a task that iterates through all the models
    in a dbt project, checks the tables exist on the db, and generates
    placeholder model descriptor files (schema.yml) for any column or models
    that have not yet been documented.
    """

    def __init__(
        self,
        flags: FlagParser,
        dbt_path: Path,
        sugar_config: DbtSugarConfig,
        dbt_profile: DbtProfile,
    ) -> None:
        # we specifically run the super init because we need to populate the cache
        # of all dbt models, where they live etc
        super().__init__(
            flags=flags, dbt_profile=dbt_profile, config=sugar_config, dbt_path=dbt_path
        )
        self.dbt_models_dict: Dict[str, Union[Path, List[str]]] = {}
        self._dbt_profile = dbt_profile
        self.schema = self._dbt_profile.profile.get("target_schema", "")

        self.dbt_models_data: List[DbtModelsDict] = []

    def build_all_models_dict(self) -> None:
        """Walk through all .sql files and load their info (name, path etc) into a dict."""
        _dbt_models_data = []
        for root, _, files in os.walk(self.repository_path):
            if not re.search(self._excluded_folders_from_search_pattern, root):
                _dbt_models_data.append(
                    [
                        DbtModelsDict(
                            model_name=f.replace(".sql", ""),
                            model_path=Path(root, f),
                            model_columns=[],
                        )
                        for f in sorted(files)
                        if f.lower().endswith(".sql")
                        and f.lower().replace(".sql", "")
                        not in self._sugar_config.dbt_project_info.get("excluded_models", [])
                    ]
                )
                self.dbt_models_data = functools.reduce(operator.iconcat, _dbt_models_data, [])

    def add_or_update_model_descriptor_placeholders(self, is_test: bool = False):
        connector = self.get_connector()
        for model_info in self.dbt_models_data:
            model_descriptor_content = {}
            model_info.model_columns = connector.get_columns_from_table(
                model_info.model_name,
                self.schema,
                use_describe=self._sugar_config.dbt_project_info.get(
                    "use_describe_snowflake", False
                ),
            )
            (
                model_descriptor_path,
                descriptor_file_exists,
                is_already_documented,
            ) = self.find_model_schema_file(model_name=model_info.model_name)
            if descriptor_file_exists and model_descriptor_path:
                model_descriptor_content = open_yaml(model_descriptor_path)

            model_descriptor_content = self.create_or_update_model_entry(
                is_already_documented,
                model_descriptor_content,
                model_name=model_info.model_name,
                columns_sql=model_info.model_columns,
            )
            if is_test:
                return self.order_schema_yml(model_descriptor_content)
            if model_descriptor_path:
                save_yaml(model_descriptor_path, self.order_schema_yml(model_descriptor_content))

    def run(self) -> int:
        self.build_all_models_dict()
        self.add_or_update_model_descriptor_placeholders()
        return 0
