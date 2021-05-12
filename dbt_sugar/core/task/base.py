"""API definition for Task-like objects."""
import abc
import os
import re
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

from dbt_sugar.core.clients.dbt import DbtProfile
from dbt_sugar.core.clients.yaml_helpers import open_yaml, save_yaml
from dbt_sugar.core.config.config import DbtSugarConfig
from dbt_sugar.core.connectors.postgres_connector import PostgresConnector
from dbt_sugar.core.connectors.redshift_connector import RedshiftConnector
from dbt_sugar.core.connectors.snowflake_connector import SnowflakeConnector
from dbt_sugar.core.flags import FlagParser
from dbt_sugar.core.logger import GLOBAL_LOGGER as logger

COLUMN_NOT_DOCUMENTED = "No description for this column."
MODEL_NOT_DOCUMENTED = "No description for this model."
DEFAULT_EXCLUDED_FOLDERS_PATTERN = r"\/target\/|\/dbt_modules\/"
DEFAULT_EXCLUDED_YML_FILES = r"dbt_project.yml|packages.yml"


DB_CONNECTORS = {
    "postgres": PostgresConnector,
    "snowflake": SnowflakeConnector,
    "redshift": RedshiftConnector,
}


class BaseTask(abc.ABC):
    """Sets up basic API for task-like classes."""

    def __init__(
        self,
        flags: FlagParser,
        dbt_path: Path,
        sugar_config: DbtSugarConfig,
        dbt_profile: DbtProfile,
    ) -> None:
        self.repository_path = dbt_path
        self._sugar_config = sugar_config
        self._flags = flags
        self._dbt_profile = dbt_profile

        # populated by class methods
        self._excluded_folders_from_search_pattern: str = self.setup_paths_exclusion()
        self.all_dbt_models: Dict[str, Path] = {}
        self.dbt_definitions: Dict[str, str] = {}
        self.dbt_tests: Dict[str, List[Dict[str, Any]]] = {}
        self.build_descriptions_dictionary()

    def get_connector(self) -> Union[PostgresConnector, SnowflakeConnector, RedshiftConnector]:
        dbt_credentials = self._dbt_profile.profile
        connector = DB_CONNECTORS.get(dbt_credentials.get("type", ""))
        if not connector:
            raise NotImplementedError(
                f"Connector '{dbt_credentials.get('type')}' is not implemented."
            )

        return connector(dbt_credentials)

    def setup_paths_exclusion(self) -> str:
        """Appends excluded_folders to the default folder exclusion patten."""
        if self._sugar_config.dbt_project_info["excluded_folders"]:
            excluded_folders_from_search_pattern: str = r"\/|\/".join(
                self._sugar_config.dbt_project_info["excluded_folders"]
            )
            return fr"{DEFAULT_EXCLUDED_FOLDERS_PATTERN}|\/{excluded_folders_from_search_pattern}\/"

        else:
            return DEFAULT_EXCLUDED_FOLDERS_PATTERN

    def get_column_description_from_dbt_definitions(self, column_name: str) -> str:
        """Searches for the description of a column in all the descriptions in DBT.

        Args:
            column_name (str): column name to get the description from.

        Returns:
            str: with the description of the column.
        """
        return self.dbt_definitions.get(column_name, COLUMN_NOT_DOCUMENTED)

    def get_documented_columns(
        self, schema_content: Dict[str, Any], model_name: str
    ) -> Dict[str, str]:
        """Method to get the documented columns from a model in a schema.yml.

        Args:
            content (Dict[str, Any]): content of the schema.yml.
            model_name (str): model name to get the columns from.

        Returns:
            Dict[str, str]: with the columns names and descriptions documented.
        """
        documented_columns = {}
        for model in schema_content.get("models", []):
            if model["name"] == model_name:
                for column in model.get("columns", []):
                    if column.get("description", COLUMN_NOT_DOCUMENTED) != COLUMN_NOT_DOCUMENTED:
                        documented_columns[column["name"]] = column["description"]
        return documented_columns

    def column_has_primary_key_tests(
        self, schema_content: Dict[str, Any], model_name: str, column_name: str
    ) -> Optional[bool]:
        """Method to check that the column with the primary key have the unique and not_null tests.

        Args:
            schema_content (Dict[str, Any]): content of the schema.yml.
            model_name (str): model name to check.
            column_name (str): column name with the primary key.

        Returns:
            Optional[bool]: True if the column have unique and not_null tests,
                False if is missing one of them, None if the column don't exists.
        """
        for model in schema_content.get("models", []):
            if model["name"] == model_name:
                for column in model.get("columns", []):
                    if column.get("name", "") == column_name:
                        column_tests = column.get("tests", [])
                        return "unique" in column_tests and "not_null" in column_tests
        return None

    def get_not_documented_columns(
        self, schema_content: Dict[str, Any], model_name: str
    ) -> Dict[str, str]:
        """Method to get the undocumented columns from a model in a schema.yml.

        Args:
            schema_content (Dict[str, Any]): content of the schema.yml.
            model_name (str): model name to get the columns from.

        Returns:
            Dict[str, str]: with the columns names and descriptions undocumented.
        """
        not_documented_columns = {}
        for model in schema_content.get("models", []):
            if model["name"] == model_name:
                for column in model.get("columns", []):
                    if column.get("description", COLUMN_NOT_DOCUMENTED) == COLUMN_NOT_DOCUMENTED:
                        not_documented_columns[column["name"]] = COLUMN_NOT_DOCUMENTED
        return not_documented_columns

    def combine_two_list_without_duplicates(self, list1: List[Any], list2: List[Any]) -> List[Any]:
        """
        Method to combine two list without duplicates.

        Args:
            list1 (List[Any]): First list with any value.
            list2 (List[Any]): Second list with any value.

        Returns:
            List[Any]: with the combine lists.
        """
        if not list1:
            return list2

        for item in list1:
            if item not in list2:
                list2.append(item)
        return list2

    def update_model_description_test_tags(
        self,
        path_file: Path,
        model_name: str,
        dict_column_description_to_update: Dict[str, Dict[str, Any]],
    ):
        """
        Method to update a schema.yml with a Dict of columns names, tests, and tags.

        Args:
            path_file (Path): Path of the schema.yml file to update.
            model_name (str): Name of the model to update.
            dict_column_description_to_update (Dict[str, Dict[str, Any]]): Dict with the column name with
            the description, tags and tests to update.
        """
        content = open_yaml(path_file)
        for model in content.get("models", []):
            if model["name"] == model_name:
                for column in model.get("columns", []):
                    column_name = column["name"]
                    if column_name in dict_column_description_to_update:
                        # Update the description
                        description = dict_column_description_to_update[column_name].get(
                            "description"
                        )
                        if description:
                            column["description"] = description

                        # Update the tests without duplicating them.
                        tests = dict_column_description_to_update[column_name].get("tests")
                        if tests:
                            column["tests"] = self.combine_two_list_without_duplicates(
                                column.get("tests", []), tests
                            )

                        # Update the tags without duplicating them.
                        tags = dict_column_description_to_update[column_name].get("tags")
                        if tags:
                            column["tags"] = self.combine_two_list_without_duplicates(
                                column.get("tags", []), tags
                            )
        save_yaml(path_file, content)

    def update_column_description_from_schema(
        self, path_file: Path, dict_column_description_to_update: Dict[str, Dict[str, Any]]
    ) -> None:
        """Method to update a schema.yml with a Dict of columns names and description.

        Args:
            path_file (Path): Path to the schema.yml file to update the columns descriptions from.
            dict_column_description_to_update (Dict[str, Dict[str, Any]]): Dict with the column name with
            the description to update.
        """
        content = open_yaml(path_file)
        for model in content.get("models", []):
            for column in model.get("columns", []):
                column_name = column["name"]
                if column_name in dict_column_description_to_update:
                    new_desctiption = dict_column_description_to_update[column_name].get(
                        "description"
                    )
                    if new_desctiption:
                        column["description"] = new_desctiption
        save_yaml(path_file, content)

    def update_column_descriptions(
        self, dict_column_description_to_update: Dict[str, Dict[str, Any]]
    ) -> None:
        """Method to update all the schema.ymls from a dbt project with a Dict of columns names and description.

        Args:
            dict_column_description_to_update (Dict[str, Dict[str, Any]]): Dict with the column name with
            the description to update.
        """
        for root, _, files in os.walk(self.repository_path):
            if not re.search(self._excluded_folders_from_search_pattern, root):
                files = [
                    f
                    for f in files
                    if f.lower().endswith(".yml")
                    and not re.search(DEFAULT_EXCLUDED_YML_FILES, f.lower())
                ]
                for file in files:
                    path_file = Path(os.path.join(root, file))
                    self.update_column_description_from_schema(
                        path_file, dict_column_description_to_update
                    )

    def update_test_in_dbt_tests(self, model_name: str, column: Dict[str, Any]) -> None:
        """Update a column tests in the global tests dictionary.

        Args:
            model_name (str): with the model name.
            column (Dict[str, Any]): column information.
        """
        if model_name not in self.dbt_tests:
            self.dbt_tests[model_name] = [
                {"name": column["name"], "tests": column.get("tests", [])}
            ]
        else:
            self.dbt_tests[model_name].append(
                {"name": column["name"], "tests": column.get("tests", [])}
            )

    def update_description_in_dbt_descriptions(
        self, column_name: str, column_description: str
    ) -> None:
        """Update a column description in the global description dictionary.

        Args:
            column_name (str): column name to update.
            column_description (str): column description to update.
        """
        if not column_description:
            column_description = COLUMN_NOT_DOCUMENTED
        self.dbt_definitions[column_name] = column_description

    def remove_excluded_models(self, content: Dict[str, Any]) -> Optional[List[Dict[str, Any]]]:
        """Removes models that are excluded_models from the models dict"""
        models = content.get("models", [])
        # if self._sugar_config.dbt_project_info.get("excluded_models"):
        logger.debug(models)
        if models:
            return [
                model_dict
                for model_dict in models
                if model_dict["name"] not in self._sugar_config.dbt_project_info["excluded_models"]
            ]

        return None

    def read_file(self, filename_path: Path) -> str:
        """
        Method to read a file.

        Args:
            filename_path (Path): full path to the file we want to read.

        Returns:
            str: content of the file.
        """
        content = ""
        if Path(filename_path).exists():
            with open(filename_path, "r") as reader:
                content = reader.read()
        return content

    def load_descriptions_from_a_schema_file(
        self, content: Dict[str, Any], path_schema: Path
    ) -> None:
        """Load the columns descriptions from a schema.yml into the global descriptions cache.

        This cache is used so that we can homogenise descriptions across models and import
        already documented ones.

        Args:
            content (Dict[str, Any]): content of the schema.yaml.
        """
        if not content:
            return
        models = self.remove_excluded_models(content)
        if not models:
            return
        for model in models:
            self.all_dbt_models[model["name"]] = path_schema
            for column in model.get("columns", []):
                column_description = column.get("description", None)
                self.update_description_in_dbt_descriptions(column["name"], column_description)
                self.update_test_in_dbt_tests(model["name"], column)

    def get_file_path_from_sql_model(self, model_name: str) -> Optional[Path]:
        """Get the complete file path from a model name.

        Args:
            model_name (str): with the model name to find.

        Returns:
            Optional[Path]: Path of the SQL file, None if the file doens't exists.
        """
        for root, _, files in os.walk(self.repository_path):
            if not re.search(self._excluded_folders_from_search_pattern, root):
                for file_name in files:
                    file_name = file_name.lower()
                    if file_name == f"{model_name}.sql" and not re.search(
                        DEFAULT_EXCLUDED_YML_FILES, file_name
                    ):
                        return Path(os.path.join(root, file_name))
        return None

    def build_descriptions_dictionary(self) -> None:
        """Load the columns descriptions from all schema files in a dbt project.

        This is purely responsble for building the knowledge of all possible definitions.
        In other words it is independent from the documentation orchestration.
        This happens in the `doc` task
        """
        for root, _, files in os.walk(self.repository_path):
            if not re.search(self._excluded_folders_from_search_pattern, root):
                files = [
                    f
                    for f in files
                    if f.lower().endswith(".yml")
                    and not re.search(DEFAULT_EXCLUDED_YML_FILES, f.lower())
                ]
                for file in files:
                    path_file = Path(os.path.join(root, file))
                    content = open_yaml(path_file)
                    logger.debug(path_file)
                    self.load_descriptions_from_a_schema_file(content, path_file)

    def is_model_in_schema_content(self, content, model_name) -> bool:
        """Method to check if a model exists in a schema.yaml content.

        Args:
            content (Dict[str, Any]): content of the schema.yaml.
            model_name (str): model name to search.

        Returns:
            boolean: is true if the model is present in the schema.yaml.
        """
        if not content:
            return False

        return any(model["name"] == model_name for model in content.get("models", []))

    def find_model_schema_file(self, model_name: str) -> Tuple[Optional[Path], bool, bool]:
        for root, _, files in os.walk(self.repository_path):
            if not re.search(self._excluded_folders_from_search_pattern, root):
                schema_file_path = None
                model_file_found = False
                schema_file_exists = False
                is_already_documented = False
                for file in files:
                    # check the model file exists and if it does return the path
                    # of the schema.yml it's in.
                    if file == f"{model_name}.sql":
                        model_file_found = True
                        logger.debug(f"Found sql file for '{model_name}'")
                        schema_file_path = self.all_dbt_models.get(model_name, None)
                    # if it's not in a schema file, then it's not documented and we
                    # need to create a schema.yml "dummy" to place it in.
                    if not schema_file_path and model_file_found:
                        logger.debug(
                            f"'{model_name}' was not contained in a schema file. Creating one at {root}"
                        )
                        schema_file_path = Path(os.path.join(root, "schema.yml"))
                        # check whether there is a schema file already present
                        schema_file_exists = False
                        if schema_file_path.exists():
                            schema_file_exists = True
                        return schema_file_path, schema_file_exists, is_already_documented

                    if schema_file_path and model_file_found:
                        logger.debug(
                            f"'{model_name}' found in '{schema_file_path}' we'll update entry."
                        )
                        is_already_documented = True
                        schema_file_exists = True
                        return schema_file_path, schema_file_exists, is_already_documented
        return None, False, False

    def is_exluded_model(self, model_name: str) -> bool:
        if model_name in self._sugar_config.dbt_project_info.get("excluded_models", []):
            raise ValueError(
                f"You decided to exclude '{model_name}' from dbt-sugar's scope. "
                f"You run `{self._flags.task}` on it you will need to remove "
                "it from the excluded_models list in the sugar_config.yml"
            )
        return True

    @abc.abstractmethod
    def run(self) -> int:
        """Orchestrator method that calls all the needed stuff to run a documentation task."""
        ...
