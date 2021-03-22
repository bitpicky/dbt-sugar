"""API definition for Task-like objects."""
import abc
import os
import re
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from dbt_sugar.core.clients.yaml_helpers import open_yaml, save_yaml

COLUMN_NOT_DOCUMENTED = "No description for this column."
MODEL_NOT_DOCUMENTED = "No description for this model."
EXCLUDE_TARGET_FILES_PATTERN = r"\/target\/"


class BaseTask(abc.ABC):
    """Sets up basic API for task-like classes."""

    def __init__(self, dbt_path: Path) -> None:
        self.repository_path = dbt_path
        self.all_dbt_models: Dict[str, Path] = {}
        self.dbt_definitions: Dict[str, str] = {}
        self.dbt_tests: Dict[str, List[Dict[str, Any]]] = {}
        self.save_all_descriptions()

    def get_column_description_from_dbt_definitions(self, column_name: str) -> str:
        """Searches for the description of a column in all the descriptions in DBT.

        Args:
            column_name (str): column name to get the description from.

        Returns:
            str: with the description of the column.
        """
        return self.dbt_definitions.get(column_name, COLUMN_NOT_DOCUMENTED)

    def get_documented_columns(self, content: Dict[str, Any], model_name: str) -> Dict[str, str]:
        """Method to get the documented columns from a model in a schema.yml.

        Args:
            content (Dict[str, Any]): content of the schema.yml.
            model_name (str): model name to get the columns from.

        Returns:
            Dict[str, str]: with the columns names and descriptions documented.
        """
        documented_columns = {}
        for model in content.get("models", []):
            if model["name"] == model_name:
                for column in model.get("columns", []):
                    if column.get("description", COLUMN_NOT_DOCUMENTED) != COLUMN_NOT_DOCUMENTED:
                        documented_columns[column["name"]] = column["description"]
        return documented_columns

    def get_not_documented_columns(
        self, content: Dict[str, Any], model_name: str
    ) -> Dict[str, str]:
        """Method to get the undocumented columns from a model in a schema.yml.

        Args:
            content (Dict[str, Any]): content of the schema.yml.
            model_name (str): model name to get the columns from.

        Returns:
            Dict[str, str]: with the columns names and descriptions undocumented.
        """
        not_documented_columns = {}
        for model in content.get("models", []):
            if model["name"] == model_name:
                for column in model.get("columns", []):
                    if column.get("description", COLUMN_NOT_DOCUMENTED) == COLUMN_NOT_DOCUMENTED:
                        not_documented_columns[column["name"]] = COLUMN_NOT_DOCUMENTED
        return not_documented_columns

    def __combine_two_list_without_duplicates(
        self, list1: List[Any], list2: List[Any]
    ) -> List[Any]:
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
        for model in content["models"]:
            if model["name"] == model_name:
                for column in model.get("columns", []):
                    column_name = column["name"]
                    if column_name in dict_column_description_to_update.keys():
                        # Update the description
                        description = dict_column_description_to_update[column_name].get(
                            "description"
                        )
                        if description:
                            column["description"] = description

                        # Update the tests without duplicating them.
                        tests = dict_column_description_to_update[column_name].get("tests")
                        if tests:
                            column["tests"] = self.__combine_two_list_without_duplicates(
                                column.get("tests", []), tests
                            )

                        # Update the tags without duplicating them.
                        tags = dict_column_description_to_update[column_name].get("tags")
                        if tags:
                            column["tags"] = self.__combine_two_list_without_duplicates(
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
        for model in content["models"]:
            for column in model.get("columns", []):
                column_name = column["name"]
                if column_name in dict_column_description_to_update.keys():
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
            if not re.search(EXCLUDE_TARGET_FILES_PATTERN, root):
                files = [f for f in files if f == "schema.yml"]
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

    def save_descriptions_from_schema(self, content: Dict[str, Any], path_schema: Path) -> None:
        """Save the columns descriptions from a schema.yml into the global descriptions dictionary.

        Args:
            content (Dict[str, Any]): content of the schema.yaml.
        """
        if not content:
            return

        for model in content.get("models", []):
            self.all_dbt_models[model["name"]] = path_schema
            for column in model.get("columns", []):
                column_description = column.get("description", None)
                self.update_description_in_dbt_descriptions(column["name"], column_description)
                self.update_test_in_dbt_tests(model["name"], column)

    def save_all_descriptions(self) -> None:
        """Save the columns descriptions from all the dbt project."""
        for root, _, files in os.walk(self.repository_path):
            if not re.search(EXCLUDE_TARGET_FILES_PATTERN, root):
                files = [f for f in files if f == "schema.yml"]
                for file in files:
                    path_file = Path(os.path.join(root, file))
                    content = open_yaml(path_file)
                    self.save_descriptions_from_schema(content, path_file)

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

        for model in content.get("models", []):
            if model["name"] == model_name:
                return True
        return False

    def find_model_in_dbt(self, model_name: str) -> Tuple[Optional[Path], bool]:
        """
        Method to find a model name in the dbt project.

            - If we find the sql of the model but there is no schema we return the Path
            and False (to create the schema).
            - If we find the sql of the model and there is schema we return the Path and True.

        Args:
            model_name (str): model name to find in the dbt project.

        Returns:
            Tuple[Optional[Path], bool]: Optional path of the sql model if found
            and boolean indicating whether the schema.yml exists.
        """
        for root, _, files in os.walk(self.repository_path):
            if not re.search(EXCLUDE_TARGET_FILES_PATTERN, root):
                for file in files:
                    if file == f"{model_name}.sql":
                        path_file = Path(os.path.join(root, "schema.yml"))
                        if path_file.is_file():
                            return path_file, True
                        else:
                            return path_file, False
        return None, False

    @abc.abstractmethod
    def run(self) -> int:
        """Orchestrator method that calls all the needed stuff to run a documentation task."""
        ...
