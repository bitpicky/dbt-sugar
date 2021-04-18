"""Bootstrap module. Generates placeholders for all models in a dbt project."""


from pathlib import Path

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

    def __init__(self, flags: FlagParser, dbt_path: Path, sugar_config: DbtSugarConfig) -> None:
        # we specifically run the super init because we need to populate the cache
        # of all dbt models, where they live etc
        super().__init__(flags, dbt_path, sugar_config)

    def run(self) -> int:
        # collect all models in the dbt project
        # iterate through all those models
        # collect their columns
        # collect their yaml content
        # add placeholders (similar to the documentation task)
        # save yaml
        ...
