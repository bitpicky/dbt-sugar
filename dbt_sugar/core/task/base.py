"""API definition for Task-like objects."""
import abc


class BaseTask(abc.ABC):
    """Sets up basic API for task-like classes."""

    @abc.abstractmethod
    def run(self) -> int:
        """Orchestrator method that calls all the needed stuff to run a documentation task."""
        ...
