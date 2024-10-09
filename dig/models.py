import pathlib
import uuid
from enum import Enum
from typing import Self, TypeAlias

from pydantic import BaseModel, Field, field_validator

YamlSupportedType: TypeAlias = str | int | list[str] | list[int] | list[str | int]


class RetryIntervalType(str, Enum):
    CONSTANT = "constant"
    EXPONENTIAL = "exponential"


class SimpleRetryCondition(BaseModel):
    limit: int


class IntervalRetryCondition(SimpleRetryCondition):
    interval: int
    interval_type: RetryIntervalType = RetryIntervalType.CONSTANT


class Task(BaseModel):
    exports: dict[str, YamlSupportedType] | None = None
    tasks: "list[NamedTask]" = Field(default_factory=list)
    retry_condition: IntervalRetryCondition | SimpleRetryCondition | None = None

    def add_task(self, task: "NamedTask") -> Self:
        self.tasks.append(task)
        return self


class ParallelTaskConfiguration(BaseModel):
    parallel: bool = False
    limit: int | None = None


class NamedTask(Task):
    name: str


class ParallelTask(NamedTask):
    configuration: ParallelTaskConfiguration


class RepeatableTask(ParallelTask):
    name: str = "repeat"
    iterables: dict[str, list[str] | list[int]]


class DockerImageConfiguration(BaseModel):
    name: str
    tag: str = Field(
        default_factory=lambda: str(uuid.uuid4()),
    )

    def get_full_image_tag(self) -> str:
        image_tag_parts = [self.name]

        if self.tag is not None:
            image_tag_parts.append(self.tag)

        return ":".join(image_tag_parts)


class DockerVolume(BaseModel):
    host_path: str
    container_path: str


class DockerImage(BaseModel):
    configuration: DockerImageConfiguration
    volumes: list[DockerVolume] = Field(default_factory=list)

    def get_container_start_command(
        self, exports: dict[str, YamlSupportedType] | None = None
    ) -> str:
        parts = [
            "sudo docker run",
        ]

        for vol in self.volumes:
            parts.append(f"-v {vol.host_path}:{vol.container_path}")

        for digdag_var in (
            "timezone",
            "project_id",
            "task_name",
            "attempt_id",
            "session_uuid",
            "session_id",
            "session_time",
            # "last_session_time",
            # "last_executed_session_time",
            # "next_session_time",
        ):
            parts.append(
                "-e DIGDAG_%s='${%s}'"
                % (
                    digdag_var.upper(),
                    digdag_var,
                )
            )

        if exports is not None:
            for key in exports.keys():
                parts.append(
                    "-e %s='${%s}'"
                    % (
                        key,
                        key,
                    )
                )

        parts.append(self.configuration.get_full_image_tag())

        return " ".join(parts)


class CommandTask(NamedTask):
    """
    A wrapper for commands that can be executed either directly or indirectly. Commands can be run
    as shell commands, as dependent workflows, as embedded workflows, or all of the previous options
    at the same time.

    :param command: A shell command to be executed. This translates to a "sh>:" command in DigDag.
    :param dependent_workflow: A workflow file name that the current task depends on. The "require" commands start another workflow
    as part of the run of the current workflow. This leaves the workflow tracked as part of the current task list rather than
    as an arbitrarily run task.
    :param embedded_workflow: A workflow file name (including .dig extension) to be arbitrarily started. The called workflow will not be
    tracked as a task for the current workflow, and as a result, runs (and fails) independently to the current task.
    """

    command: str = Field(
        default=None,
    )
    image: DockerImage | None = Field(
        default=None,
    )
    continue_on_failure: bool = Field(
        default=False,
    )

    def get_command(
        self,
        exports: dict[str, YamlSupportedType] | None = None,
    ) -> str:
        parts: list[str] = []

        if exports is None:
            exports = {}

        if self.exports is not None:
            exports.update(self.exports)

        if self.image is not None:
            parts.append(self.image.get_container_start_command(exports))

        parts.append(self.command)

        if self.continue_on_failure:
            # Pipe into null operator to allow for "successful" exit status from shell
            parts.append("|| :")

        return " ".join(parts)


class ErrorTask(CommandTask):
    name: str = Field(default="_error")


class DockerBuildImageTask(CommandTask):
    def get_command(self, exports: dict[str, YamlSupportedType] | None = None) -> str:
        assert self.image is not None
        image_tag = self.image.configuration.get_full_image_tag()
        return f"sudo docker image inspect {image_tag} > /dev/null || sudo docker build --tag {image_tag} ."


class DependentWorkflow(NamedTask):
    workflow: str
    project: str | int | None = None


class EmbeddedTask(NamedTask):
    workflow: str

    @field_validator("workflow")
    def check_extension(cls, v: str | None) -> str | None:
        # Check the the provided embedded workflow is provided as a file name
        # rather than just the workflow name.
        if v is None:
            return v
        if not v.endswith(".dig"):
            return v + ".dig"
        return v


class CRONSchedule(BaseModel):
    minutes: str = "*"
    hours: str = "*"
    day_of_month: str = "*"
    month: str = "*"
    day_of_week: str = "*"

    def to_string(self) -> str:
        return " ".join(
            [self.minutes, self.hours, self.day_of_month, self.month, self.day_of_week]
        )


class WorkflowSchedule(BaseModel):
    daily: str | None = None
    cron: CRONSchedule | None = None
    skip_delayed_by: int | None = 1
    skip_on_overtime: bool = True


class Workflow(NamedTask):
    timezone: str = "UTC"
    schedule: WorkflowSchedule | None = None
    error: ErrorTask | None = None


class ProjectPath(BaseModel):
    root_path: pathlib.Path
    ignore_patterns: list[str] = Field(default_factory=list)


class WorkflowProject(BaseModel):
    name: str

    # Path to the root of the project. If provided, this can be used to bundle the entire
    # project together to be deployed as one workflow archive.
    project_root: ProjectPath | None = None

    workflows: list[Workflow] = Field(default_factory=list)

    image: DockerImage | None = Field(default=None)

    def add_workflow(self, workflow: Workflow) -> Self:
        self.workflows.append(workflow)
        return self
