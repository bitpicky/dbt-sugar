"""Document Task module."""
from dbt_sugar.core.flags import FlagParser
from dbt_sugar.core.logger import GLOBAL_LOGGER as logger
from dbt_sugar.core.task.base import BaseTask


class DocumentationTask(BaseTask):
    """Documentation Task object.

    Holds methods and attrs necessary to orchestrate a model documentation task.
    """

    def __init__(self, flags: FlagParser) -> None:
        ...

    def run(self) -> int:
        logger.info("Would run documentatio task")
        return 0
