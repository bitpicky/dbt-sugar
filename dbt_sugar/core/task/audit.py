"""Audit Task module."""
from pathlib import Path
from typing import Any, Dict, List

from rich import box
from rich.console import Console
from rich.table import Table

from dbt_sugar.core.clients.yaml_helpers import open_yaml
from dbt_sugar.core.config.config import DbtSugarConfig
from dbt_sugar.core.flags import FlagParser
from dbt_sugar.core.logger import GLOBAL_LOGGER as logger
from dbt_sugar.core.task.base import COLUMN_NOT_DOCUMENTED, BaseTask

console = Console()
NUMBER_COLUMNS_TO_PRINT_PER_ITERACTION = 5


class AuditTask(BaseTask):
    """Audit Task object.

    Holds methods and attrs necessary to audit a model or a dbt project.
    """

    def __init__(self, flags: FlagParser, dbt_path: Path, sugar_config: DbtSugarConfig) -> None:
        self.dbt_path = dbt_path
        super().__init__(flags=flags, dbt_path=self.dbt_path, sugar_config=sugar_config)
        self.column_update_payload: Dict[str, Dict[str, Any]] = {}
        self._flags = flags
        self.model_name = self._flags.model
        self.model_content: Dict[str, Any] = {}

    def run(self) -> int:
        """Main script to run the command doc"""
        if self.model_name:
            _ = self.is_exluded_model(self.model_name)
            logger.info(f"Running audit of model [bold magenta]{self.model_name}.[/bold magenta]\n")
            path_file, schema_exists, _ = self.find_model_schema_file(self.model_name)
            if not path_file:
                logger.info(f"Could not find {self.model_name} in the project at {self.dbt_path}")
                return 1
            if not schema_exists:
                logger.info("The model is not documented.")
                return 1
            self.model_content = open_yaml(path_file)
            self.derive_model_coverage()
        else:
            logger.info(f"Running audit of dbt project in {self.dbt_path}.\n")
            self.derive_project_coverage()
        return 0

    def derive_model_coverage(self) -> None:
        """Method to get the coverage from a specific model."""
        self.get_model_column_description_coverage()
        self.get_model_test_coverage()

    def derive_project_coverage(self) -> None:
        """Method to get the coverage from a dbt project."""
        self.get_project_column_description_coverage()
        self.get_project_test_coverage()

    def calculate_coverage_percentage(self, misses: int, total: int) -> str:
        """
        Method to calculate the percentage of coverage.

        Args:
            number_failures (int): With the number of failures.
            total (int): With the number of total cases.

        Returns:
            str: with the calculation of the percentage.
        """
        if total == 0:
            return "0.0"

        percentage_failure = round((1 - (misses / total)) * 100, 1)
        return str(percentage_failure)

    def get_model_test_coverage(self) -> None:
        """Method to get the tests coverage from a specific model."""
        # Init variables
        model_number_columns = 0
        model_columns_without_tests = 0
        untested_columns = []

        columns = self.dbt_tests.get(self.model_name)

        if not columns:
            logger.info(
                f"There is no documentation entry for '{self.model_name}' in your schema.yml files. "
                "You might need to run `dbt-sugar doc` first."
            )
            return

        for column in columns:
            model_number_columns += 1
            if len(column["tests"]) == 0:
                model_columns_without_tests += 1
                untested_columns.append(column["name"])

        percentage_not_tested_columns = self.calculate_coverage_percentage(
            misses=model_columns_without_tests, total=model_number_columns
        )

        data = self.print_nicely_the_data(
            data=untested_columns, total=percentage_not_tested_columns
        )

        self.create_table(
            title="Test Coverage", columns=["Untested Columns", r"% coverage"], data=data
        )

    def get_model_column_description_coverage(self) -> None:
        """Method to get the descriptions coverage from a specific model."""
        not_documented_columns = self.get_not_documented_columns(
            content=self.model_content,
            model_name=self.model_name,
        ).keys()

        number_not_documented_columns = len(not_documented_columns)
        number_documented_columns = len(
            self.get_documented_columns(
                content=self.model_content,
                model_name=self.model_name,
            )
        )
        number_columns = number_documented_columns + number_not_documented_columns

        # This means that they are not columns, and we want to skip the printing.
        if number_columns == 0:
            return

        percentage_not_documented_columns = self.calculate_coverage_percentage(
            misses=number_not_documented_columns,
            total=number_columns,
        )
        logger.debug(
            f"percentage_not_documented_columns for '{self.model_name}': {percentage_not_documented_columns}"
        )

        data = self.print_nicely_the_data(
            data=list(not_documented_columns), total=percentage_not_documented_columns
        )

        self.create_table(
            title="Documentation Coverage",
            columns=["Undocumented Columns", r"% coverage"],
            data=data,
        )

    def print_nicely_the_data(self, data: List[str], total: str) -> Dict[str, str]:
        """
        Transforms a list into a dictionary (key: column, value: coverahe) total is at the end.

        Args:
            data (List): list of data to modify.

        Returns:
            Dict[str, str]: with a dictionary with the data as keys and
            the total as a value but only for the last element in the list.
        """
        if data:
            reshaped_data = {column: "" for column in data}
            reshaped_data[""] = ""
            reshaped_data["Total"] = total
            return reshaped_data
        if not data and total == "100.0":
            reshaped_data = {}
            reshaped_data["None"] = ""
            reshaped_data[""] = ""
            reshaped_data["Total"] = total
            return reshaped_data
        return {}

    def create_table(self, title: str, columns: List[str], data: Dict[str, str]) -> None:
        """
        Method to create a nice table to print the results.

        Args:
            title (str): Title that you want to give to the table.
            columns (List[str]): List of columns that the table is going to have.
            data (Dict[str, str]): with the rows that we want to print.
        """
        table = Table(title=title, box=box.SIMPLE)
        for column in columns:
            table.add_column(column, justify="right", style="bright_yellow", no_wrap=True)

        for model, percentage in data.items():
            table.add_row(model, percentage)

        console = Console()
        console.print(table)

    def get_project_test_coverage(self) -> None:
        """Method to get the model tests coverage per model in a dbt project."""
        print_statistics = {}
        total_number_columns = 0
        number_columns_without_tests = 0

        for model_name in self.dbt_tests.keys():
            columns = self.dbt_tests[model_name]

            model_number_columns = 0
            model_columns_without_tests = 0

            for column in columns:
                total_number_columns += 1
                model_number_columns += 1

                if len(column["tests"]) == 0:
                    number_columns_without_tests += 1
                    model_columns_without_tests += 1

            print_statistics[model_name] = self.calculate_coverage_percentage(
                misses=model_columns_without_tests, total=model_number_columns
            )

        print_statistics[""] = ""
        print_statistics["Total"] = self.calculate_coverage_percentage(
            misses=number_columns_without_tests, total=total_number_columns
        )

        self.create_table(
            title="Test Coverage",
            columns=["Model Name", r"% coverage"],
            data=print_statistics,
        )

    def get_project_column_description_coverage(self) -> None:
        """Method to get the model descriptions coverage per model in a dbt project."""
        print_statistics = {}
        for model_name, path in self.all_dbt_models.items():
            content = open_yaml(path)

            number_documented_columns = len(
                self.get_documented_columns(
                    content=content,
                    model_name=model_name,
                )
            )

            number_not_documented_columns = len(
                self.get_not_documented_columns(
                    content=content,
                    model_name=model_name,
                )
            )

            print_statistics[model_name] = self.calculate_coverage_percentage(
                misses=number_not_documented_columns,
                total=(number_documented_columns + number_not_documented_columns),
            )

        print_statistics[""] = ""
        print_statistics["Total"] = self.get_project_total_test_coverage()

        self.create_table(
            title="Documentation Coverage",
            columns=["Model Name", r"% coverage"],
            data=print_statistics,
        )

    def get_project_total_test_coverage(self) -> str:
        """
        Method to get the descriptions coverage for an entire dbt project.

        Returns:
            str: with global descriptions statistics.
        """
        number_not_documented_columns = 0
        number_of_columns = 0

        for description in self.dbt_definitions.values():
            if description == COLUMN_NOT_DOCUMENTED:
                number_not_documented_columns += 1
            number_of_columns += 1

        return self.calculate_coverage_percentage(
            misses=number_not_documented_columns,
            total=number_of_columns,
        )
