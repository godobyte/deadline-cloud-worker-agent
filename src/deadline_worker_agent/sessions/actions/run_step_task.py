# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from __future__ import annotations
from concurrent.futures import Executor
from typing import Any, TYPE_CHECKING

from openjd.sessions import Parameter

from .openjd_action import OpenjdAction

if TYPE_CHECKING:
    from ..job_entities import StepDetails
    from ..session import Session


class RunStepTaskAction(OpenjdAction):
    """Action to run a step's task within a Worker session

    Parameters
    ----------
    id : str
        A unique identifier for the session action
    step_id : str
        The unique step identifier
    details : StepDetails
        The environment details
    task_id : str
        The unique task identifier
    task_parameter_values : list[Parameter]
        The task parameter values
    """

    step_id: str
    task_id: str
    _details: StepDetails
    _task_parameter_values: list[Parameter]

    def __init__(
        self,
        *,
        id: str,
        step_id: str,
        details: StepDetails,
        task_id: str,
        task_parameter_values: list[Parameter],
    ) -> None:
        super(RunStepTaskAction, self).__init__(
            id=id,
        )
        self._details = details
        self.step_id = step_id
        self.task_id = task_id
        self._task_parameter_values = task_parameter_values

    def __eq__(self, other: Any) -> bool:
        return (
            type(self) == type(other)
            and self._id == other._id
            and self.step_id == other.step_id
            and self.task_id == other.task_id
            and self._details == other._details
            and self._task_parameter_values == other._task_parameter_values
        )

    def start(self, *, session: Session, executor: Executor) -> None:
        """Initiates the running of a step's task in the session

        Parameters
        ----------
        session : Session
            The Session that is the target of the action
        executor : Executor
            An executor for running futures
        """
        session.run_task(
            step_script=self._details.script,
            task_parameter_values=self._task_parameter_values,
        )

    def human_readable(self) -> str:
        param_str = ", ".join(
            f"{param.name}={repr(param.value)}" for param in self._task_parameter_values
        )
        return f"step[{self.step_id}].run({param_str})"