import uuid
import requests
from typing import Any

from ..dig import dig
from .models import (
    Attempts,
    ProjectRevisions,
    SessionAttempts,
    WorkflowAttempt,
    Workflows,
    Workflow,
    Attempt,
    AttemptParameters,
    Project,
    Projects,
    Session,
    Sessions,
)


class DigdagClient:
    def __init__(
        self,
        host: str,
    ) -> None:
        if host.endswith("/"):
            host = host.rstrip("/")

        if not host.endswith("/api"):
            host = host + "/api"

        self.host: str = host

    def _make_url(
        self,
        *path_parts: str | int,
    ) -> str:
        full_path = "/".join([str(item) for item in path_parts])
        if not full_path.startswith("/"):
            full_path = "/" + full_path
        return self.host + full_path

    def _get(
        self,
        url: str,
        headers: dict[str, Any] | None = None,
        params: dict[str, Any] | None = None,
    ) -> requests.Response:
        return requests.get(
            url,
            headers=headers,
            params=params,
        )

    def _delete(
        self,
        url: str,
        headers: dict[str, Any] | None = None,
        params: dict[str, Any] | None = None,
    ) -> requests.Response:
        return requests.delete(
            url,
            headers=headers,
            params=params,
        )

    def _post(
        self,
        url: str,
        headers: dict[str, Any] | None = None,
        params: dict[str, Any] | None = None,
        data: Any = None,
    ) -> requests.Response:
        return requests.post(
            url,
            headers=headers,
            params=params,
            data=data,
        )

    def _put(
        self,
        url: str,
        headers: dict[str, Any] | None = None,
        params: dict[str, Any] | None = None,
        data: Any = None,
    ) -> requests.Response:
        return requests.put(
            url,
            headers=headers,
            params=params,
            data=data,
        )

    def get_workflows(self) -> Workflows:
        return Workflows.model_validate(
            self._get(
                self._make_url("/workflows"),
            ).json()
        )

    def get_workflow(
        self,
        id: str,
    ) -> Workflow:
        response = self._get(
            self._make_url(
                "/workflows",
                id,
            ),
        )
        return Workflow.model_validate(response.json())

    def get_workflow_by_name(
        self,
        name: str,
    ) -> Workflow:
        return self.get_workflows().filter_by_name(name)

    def get_sessions(self) -> Sessions:
        response = self._get(
            self._make_url(
                "/sessions",
            )
        )
        return Sessions.model_validate(response.json())

    def get_session(
        self,
        id: str,
    ) -> Session:
        response = self._get(
            self._make_url(
                "/sessions",
                id,
            )
        )
        return Session.model_validate(response.json())

    def get_session_attempts(
        self,
        id: str,
    ) -> SessionAttempts:
        response = self._get(
            self._make_url(
                "sessions",
                id,
                "attempts",
            )
        )
        return SessionAttempts.model_validate(response.json())

    def get_attempts(self) -> Attempts:
        response = self._get(
            self._make_url(
                "/attempts",
            )
        )
        return Attempts.model_validate(response.json())

    def get_attempts_by_workflow_name(
        self,
        name: str,
    ) -> Attempts:
        attempts = self.get_attempts()
        return Attempts(
            attempts=[item for item in attempts.attempts if item.workflow.name == name],
        )

    def get_attempt(
        self,
        id: str,
    ) -> WorkflowAttempt:
        response = self._get(
            self._make_url(
                "attempts",
                id,
            )
        )
        return WorkflowAttempt.model_validate(response.json())

    def start_attempt(
        self,
        parameters: AttemptParameters,
    ) -> Attempt:
        response = self._put(
            self._make_url("/attempts"),
            headers={
                "Content-Type": "application/json",
            },
            data=parameters.model_dump_json(
                by_alias=True,
            ),
        )

        if response.status_code != 200:
            raise RuntimeError(
                f"Digdag API returned an error status code ({response.status_code}).\n\n{response.text}"
            )
        return Attempt.model_validate(response.json())

    def _upload_project_archive(
        self,
        content: bytes,
        project_name: str,
        revision: str,
        schedule_from: str | None = None,
    ) -> Project:
        params = {
            "project": project_name,
            "revision": revision,
        }

        if schedule_from is not None:
            params["schedule_from"] = schedule_from

        response = self._put(
            self._make_url("/projects"),
            headers={
                "Content-Type": "application/gzip",
            },
            params=params,
            data=content,
        )

        if response.status_code not in range(200, 300):
            raise RuntimeError(response.text)

        return Project.model_validate(response.json())

    def upload_project(
        self,
        project: dig.WorkflowProject,
        revision: str | None = None,
        schedule_from: str | None = None,
    ) -> Project:
        revision = revision or str(uuid.uuid4())
        tarball_content = dig.create_project_archive(project)
        return self._upload_project_archive(
            tarball_content, project.name, revision, schedule_from
        )

    def delete_project(
        self,
        id: str,
    ) -> Project:
        response = self._delete(
            self._make_url(
                "projects",
                id,
            ),
        )
        return Project.model_validate(response.json())

    def get_projects(
        self,
        name: str | None = None,
    ) -> Projects:
        params = {}

        if name is not None:
            params["name"] = name

        response = self._get(
            self._make_url(
                "/projects",
            ),
            params=params,
        )
        return Projects.model_validate(response.json())

    def get_project(
        self,
        id: str,
    ) -> Project:
        response = self._get(
            self._make_url(
                "projects",
                id,
            ),
        )
        if response.status_code == 404:
            raise ValueError(f"No matching project found for ID <{id}>")
        return Project.model_validate(response.json())

    def get_project_by_name(
        self,
        name: str,
    ) -> Project:
        return self.get_projects().filter_by_name(name)

    def get_project_workflows(
        self,
        id: str,
    ) -> Workflows:
        response = self._get(
            self._make_url(
                "projects",
                id,
                "workflows",
            ),
        )
        return Workflows.model_validate(response.json())

    def get_project_workflow_by_name(
        self,
        name: str,
        workflow_name: str,
    ) -> Workflow:
        project = self.get_project_by_name(name)
        response = self._get(
            self._make_url(
                "projects",
                project.id,
                "workflows",
                workflow_name,
            )
        )
        return Workflow.model_validate(response.json())

    def get_project_revisions(self, id: str) -> ProjectRevisions:
        response = self._get(
            self._make_url(
                "projects",
                id,
                "revisions",
            )
        )
        return ProjectRevisions.model_validate(response.json())
