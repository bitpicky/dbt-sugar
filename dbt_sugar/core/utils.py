"""Utility functions. Things that don't really belong anywhere specific so they end up there..."""

from typing import Optional, Tuple
from urllib.error import URLError

import luddite
from packaging.version import parse as semver_parse

from dbt_sugar.core._version import __version__
from dbt_sugar.core.logger import GLOBAL_LOGGER as logger


def check_and_compare_version(external_version: Optional[str] = str()) -> Tuple[bool, str]:
    """Checks what the currently installed version of dbt-sugar is and compares it to the one on PyPI.

    This requires an internet connection. In the case where this doesn't happen a URLError will
    probably be thrown and in that case we just return False not to cause annoying user experience.

    Args:
        external_version (Optional[str], optional): Mainly for testing purposes. Defaults to str().

    Returns:
        bool: True when dbt-sugar needs an update. False when good.
    """
    try:
        pypi_version: str = luddite.get_version_pypi("dbt-sugar")
        if external_version:
            installed_version = external_version
        else:
            installed_version = __version__

        needs_update = semver_parse(pypi_version) > semver_parse(installed_version)
        if needs_update:
            logger.warning(
                f"[yellow]Looks like you're a bit behind. A newer version of dbt-sugar v{pypi_version} "
                "is available."
            )
        return needs_update, pypi_version

    except URLError:
        return False, str()
