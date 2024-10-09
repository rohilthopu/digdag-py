from uuid import UUID
from pydantic import BaseModel, Field, ConfigDict
from typing import Any, Generator, Iterator
from datetime import datetime, UTC


class AttemptParameters(BaseModel):
    session_time: datetime = Field(
        alias="sessionTime",
    )
    workflow_id: str = Field(
        ...,
        alias="workflowId",
    )
    params: dict[str, Any] = Field(
        default_factory=dict,
    )

    model_config = ConfigDict(
        populate_by_name=True,
    )


class ProjectReference(BaseModel):
    id: str
    name: str


class WorkflowReference(BaseModel):
    id: str
    name: str

    def create_attempt_parameters(
        self,
        session_time: datetime | None = None,
        params: dict[str, Any] | None = None,
    ) -> AttemptParameters:
        if session_time is None:
            session_time = datetime.now(UTC)

        if params is None:
            params = {}

        return AttemptParameters.model_validate(
            {
                "session_time": session_time,
                "workflow_id": self.id,
                "params": params,
            }
        )


class Workflow(WorkflowReference):
    project: ProjectReference
    revision: str
    timezone: str


class Attempt(BaseModel):
    id: str = Field(
        ...,
    )
    is_finished: bool = Field(
        ...,
        alias="done",
    )
    is_successful: bool = Field(
        ...,
        alias="success",
    )
    created_at: datetime = Field(
        ...,
        alias="createdAt",
    )
    finished_at: datetime | None = Field(
        None,
        alias="finishedAt",
    )
    retry_attempt_name: str | None = Field(
        default=None,
        validation_alias="retryAttemptName",
    )
    cancel_requested: bool = Field(
        default=False,
        validation_alias="cancelRequested",
    )
    params: dict[str, Any] = Field(
        default_factory=dict,
    )

    model_config = ConfigDict(
        populate_by_name=True,
    )


class WorkflowAttempt(Attempt):
    workflow: WorkflowReference = Field(
        ...,
    )


class Attempts(BaseModel):
    attempts: list[WorkflowAttempt]


class Workflows(BaseModel):
    workflows: list[Workflow]

    def __iter__(self) -> Generator[Workflow, None, None]:
        for workflow in self.workflows:
            yield workflow

    def filter_by_project_name(self, name: str) -> Workflow:
        for item in self.workflows:
            if item.project.name == name:
                return item
        raise RuntimeError("No workflow found for project name: %s" % name)

    def filter_by_project_id(self, id: str) -> Workflow:
        for item in self.workflows:
            if item.project.id == id:
                return item
        raise RuntimeError("No workflow found for project id: %s" % id)

    def filter_by_id(self, id: str) -> Workflow:
        for item in self.workflows:
            if item.id == id:
                return item
        raise RuntimeError("No workflow found for id: %s" % id)

    def filter_by_name(self, name: str) -> Workflow:
        for item in self.workflows:
            if item.name == name:
                return item
        raise RuntimeError("No workflow found for name: %s" % name)

    def filter_by_revision(self, revision: str) -> Workflow:
        for item in self.workflows:
            if item.revision == revision:
                return item
        raise RuntimeError("No workflow found for revision: %s" % revision)


class ProjectRevision(BaseModel):
    revision: str
    created_at: datetime = Field(
        alias="createdAt",
    )
    archive_type: str = Field(
        alias="archiveType",
    )
    archive_md5: str = Field(
        alias="archiveMd5",
    )
    user_info: dict[str, Any] = Field(
        default_factory=dict,
    )


class ProjectRevisions(BaseModel):
    revisions: list[ProjectRevision]


class Project(ProjectReference):
    id: str
    name: str
    revision: str
    created_at: datetime = Field(
        alias="createdAt",
    )
    update_date: datetime = Field(
        alias="updatedAt",
    )
    deleted_at: datetime | None = Field(
        default=None,
        alias="deletedAt",
    )
    archive_type: str = Field(
        alias="archiveType",
    )
    archive_md5: str = Field(
        alias="archiveMd5",
    )


class Projects(BaseModel):
    projects: list[Project]

    def __iter__(self) -> Iterator[Project]:
        for project in self.projects:
            yield project

    def filter_by_name(self, name: str) -> Project:
        for project in self.projects:
            if project.name == name:
                return project

        raise RuntimeError("No project found for name: %s" % name)


class Session(BaseModel):
    id: str
    project: ProjectReference
    workflow: WorkflowReference
    session_uuid: UUID = Field(
        ...,
        validation_alias="sessionUuid",
    )
    session_time: datetime = Field(
        ...,
        validation_alias="sessionTime",
    )
    last_attempt: Attempt = Field(
        ...,
        validation_alias="lastAttempt",
    )


class Sessions(BaseModel):
    sessions: list[Session]


class SessionAttempts(BaseModel):
    attempts: list[WorkflowAttempt]
