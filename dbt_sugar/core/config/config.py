"""Holds config for dbt-sugar."""

from dbt_sugar.core.flags import FlagParser


class DbtSugarConfig:
    """dbt-sugar configuration class."""

    def __init__(self, flags: FlagParser) -> None:
        """Constructor for DbtSugarConfig.

        Args:
            flags (FlagParser): consumed flags from FlagParser object.
        """
        ...
