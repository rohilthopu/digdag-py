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
    """
    High level wrapper for the "_retry" directive of a Digdag workflow.
    """

    interval: int
    interval_type: RetryIntervalType = RetryIntervalType.CONSTANT


class Task(BaseModel):
    """
    Lowest level Task construct wrapper. This should not be used directly.
    """

    exports: dict[str, YamlSupportedType] | None = Field(
        default=None,
    )
    tasks: "list[NamedTask]" = Field(
        default_factory=list,
    )
    retry_condition: IntervalRetryCondition | SimpleRetryCondition | None = Field(
        default=None,
    )

    def add_task(self, task: "NamedTask") -> Self:
        self.tasks.append(task)
        return self


class ParallelTaskConfiguration(BaseModel):
    """
    Wrapper for the "_parallel" directive in Digdag.

    One of parallel/limit must be provided if a task is marked as parallel.
    """

    parallel: bool = False
    limit: int | None = None


class NamedTask(Task):
    """
    Wrapper for groups of tasks. Any task type can be enclosed inside a NamedTask in order to hierarchize a group
    of tasks based on requirements.
    """

    name: str


class ParallelTask(NamedTask):
    configuration: ParallelTaskConfiguration


class RepeatableTask(ParallelTask):
    name: str = "repeat"
    iterables: dict[str, list[str] | list[int]]


class DockerImageConfiguration(BaseModel):
    """
    High level wrapper for a Docker image tagging configuration.

    When built, an image will be created with the given name and tagged with the provided tag. "name:tag"

    By default, if a tag is not provided, a UUIDv4 value will be automatically generated.
    """

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
    """
    High level wrapper for the DockerVolume "-v" flag syntax.
    """

    host_path: str
    container_path: str


class DockerImage(BaseModel):
    """
    High level Docker image creation/start configuration that can be used for project/task execution.

    If a Docker image instance is provided, any command will be executed inside of the running container.

    Additionally, any extra parameters will be explicitly passed to the command using the "-e" env directive.
    """

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
            # The following fields only exist in scheduled jobs. It is possible to pass this information downward from
            # the WorkflowProject instance but for now, they're being ignore from the environment.
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

        # Additionally, pass any extra exports (recursively overwriting values) from the specified exports for
        # a task/project/workflow.
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
    A wrapper for the sh>: operator in Digdag.

    Any valid shell command supported by the system that Digdag is running on is supported.
    """

    command: str = Field(
        default=None,
    )

    # Optionally link a DockerImage configuration so that a proper output shell command can be generated when
    # creating the workflow files. If an image is provided, the provided shell command will be executed inside of
    # the specific container.
    image: DockerImage | None = Field(
        default=None,
    )

    # Fake functionality since Digdag does not support this.

    # Attempt to support continuing workflow execution when a task fails by piping the output of the
    # command into nil. As far as the user is concerned, this is just a boolean flag that can be set. The tradeoff
    # though is that the task will show successful in the Digdag task UI.
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
    """
    Dedicated Task type used for error handling. Note that although the paramaters for this are the same as NamedTask,
    only the command proeperty will be considered.

    Name is not required. Any names will be overwritten with "_error" in the generated .dig file.
    """

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
    """
    High level wrapper for possible Workflow schedule options.

    The schedule write priority is based on individual components first, then CRON.

    Ex. Minute Interval -> Hourly -> Daily -> Weekly -> Monthly -> CRON

    CRON scheduling should be specific using the CRONSchedule wrapper.
    """

    daily: str | None = None
    cron: CRONSchedule | None = None
    skip_delayed_by: int | None = 1
    skip_on_overtime: bool = True


class Workflow(NamedTask):
    """
    High level construct to wrap any task type into a named Workflow.
    """

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
