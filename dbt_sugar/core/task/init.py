"""Init task module."""

from dbt_sugar.core.task.base import BaseTask


class InitTask(BaseTask):
    """Init Task class object.

    Holds methods and classes to run the init operation which helps set up the dbt-sugar env.

    # ! MAINTENANCE: This may not be needed so we'll remove it if so.
    """

    def __init__(self) -> None:
        """Constructor for dbt-sugar init task."""
        ...
