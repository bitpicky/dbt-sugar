"""Bootstrap module. Generates placeholders for all models in a dbt project."""


import os
import re
from pathlib import Path
from typing import Dict, List, Union

from dbt_sugar.core.clients.dbt import DbtProfile
from dbt_sugar.core.clients.yaml_helpers import open_yaml, save_yaml
from dbt_sugar.core.config.config import DbtSugarConfig
from dbt_sugar.core.flags import FlagParser
from dbt_sugar.core.task.doc import DocumentationTask


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
        self.dbt_models_dict: Dict[str, Dict[str, Union[None, Path, str, bool, List[str]]]] = {}
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

    def add_or_update_model_descriptor_placeholders(self):
        connector = self.get_connector()
        for model, model_info in self.dbt_models_dict.items():
            model_descriptor_content = {}
            model_info["columns"] = connector.get_columns_from_table(
                model,
                self.schema,
                use_describe=self._sugar_config.dbt_project_info.get(
                    "use_describe_snowflake", False
                ),
            )
            (
                model_descriptor_path,
                descriptor_file_exists,
                is_already_documented,
            ) = self.find_model_schema_file(model_name=model)
            if descriptor_file_exists and model_descriptor_path:
                model_descriptor_content = open_yaml(model_descriptor_path)

            model_descriptor_content = self.create_or_update_model_entry(
                is_already_documented,
                model_descriptor_content,
                model_name=model,
                columns_sql=model_info["columns"],
            )
            save_yaml(model_descriptor_path, self.order_schema_yml(model_descriptor_content))

    # def add_placeholders(self):
    # for model, model_info in self.dbt_models_dict.items():
    # # if model is described, we want to check whether the columns are described
    # if not model_info["descriptor_file_exists"]:
    # content = self.create_or_update_model_entry(
    # is_already_documented=model_info["is_already_documented"],
    # content=None,
    # model_name=model,
    # column_sql=model_info["column"],
    # )
    # if model_info["descriptor_file_exists"]:
    # model_descriptor_content = open_yaml(model_info["model_descriptor_path"])  # noqa
    # not_documented_columns = self.get_not_documented_columns(content, model)  # noqa
    # documented_columns = self.get_documented_columns(content, model)  # noqa
    # ...

    def run(self) -> int:
        self.build_all_models_dict()
        self.add_or_update_model_descriptor_placeholders()
        return 0

        # collect all models in the dbt project --done
        # iterate through all those models --done
        # check the models exist in the db --done
        # collect their columns from the db --done
        # collect their yaml content from the schema.yml
        # add placeholders (similar to the documentation task) --done
        # save yaml --done

        # TODO: Check the case when a model doesn't exist in the db. Does it break?? Do we just get nothing?
        # TODO: Could we make the check columns in db and add model descriptor path be async.
