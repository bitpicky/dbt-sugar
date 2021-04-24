"""Bootstrap module. Generates placeholders for all models in a dbt project."""


import os
import re
from pathlib import Path
from typing import Dict, Sequence, Union

from dbt_sugar.core.clients.dbt import DbtProfile
from dbt_sugar.core.config.config import DbtSugarConfig
from dbt_sugar.core.flags import FlagParser
from dbt_sugar.core.task.base import BaseTask


class BootstrapTask(BaseTask):
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
        super().__init__(flags, dbt_path, sugar_config, dbt_profile)
        self.dbt_models_dict: Dict[str, Dict[str, Union[Path, str, Sequence[str]]]] = {}
        self._dbt_profile = dbt_profile
        self.schema = self._dbt_profile.profile.get("target_schema", "")

    def build_all_models_dict(self) -> None:
        """Walk through all .sql files and load their info (name, path etc) into a dict."""
        for root, _, files in os.walk(self.repository_path):
            if not re.search(self._excluded_folders_from_search_pattern, root):
                self.dbt_models_dict.update(
                    {
                        f.replace(".sql", ""): {"path": Path(root, f)}
                        for f in files
                        if f.lower().endswith(".sql")
                        and f.lower().replace(".sql", "")
                        not in self._sugar_config.dbt_project_info.get("excluded_models", [])
                    }
                )

    def check_colums_in_db(self):
        connector = self.get_connector()
        for model, model_info in self.dbt_models_dict.items():
            model_info["columns"] = connector.get_columns_from_table(
                model,
                self.schema,
                use_describe=self._sugar_config.dbt_project_info.get(
                    "use_describe_snowflake", False
                ),
            )

    def add_model_descriptor_path(self):
        for model, model_info in self.dbt_models_dict.items():
            model_info["model_descriptor_path"] = self.find_model_schema_file(model_name=model)

    def run(self) -> int:
        # collect all models in the dbt project --done
        # iterate through all those models --done
        # check the models exist in the db --done
        # collect their columns from the db --done
        # collect their yaml content from the schema.yml
        # add placeholders (similar to the documentation task)
        # save yaml

        # TODO: Check the case when a model doesn't exist in the db. Does it break?? Do we just get nothing?
        # TODO: Could we make the check columns in db and add model descriptor path be async.
        ...
