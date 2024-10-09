import uuid
import requests
from typing import Any

from ..dig.exporters import WorkflowExporter, ProjectArchiver
from ..dig.models import WorkflowProject
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
        # Very weak check that the provided host is even remotely correct. Okay to assume that
        # the user provides the correct base url for their digdag instance, optionally already
        # pointing to the /api path.
        if host.endswith("/"):
            host = host.rstrip("/")

        if not host.endswith("/api"):
            host = host + "/api"

        self.host: str = host

    def _make_url(
        self,
        *parts: str | int,
    ) -> str:
        # Digdag API paths are pretty predictable, so just shortcut their construction by joining
        # a bunch of parts, either str/int, together by a back slash.
        full_path = "/".join([str(item).lstrip("/") for item in parts])
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

    # Collection endpoint for all workflows in the entire Digdag instance. These are wrapped in
    # a container object in the API response and is also done so here.
    def get_workflows(self) -> Workflows:
        """
        Retreive all workflows for all projects in the Digdag instance.
        """
        response = self._get(
            self._make_url(
                "/workflows",
            ),
        )
        return Workflows.model_validate(response.json())

    def get_workflow(
        self,
        id: str,
    ) -> Workflow:
        """
        Retrieve an individual workflow by its ID.
        """
        response = self._get(
            self._make_url(
                "/workflows",
                id,
            ),
        )
        return Workflow.model_validate(response.json())

    def get_sessions(self) -> Sessions:
        """
        Get all workflow sessions.
        """
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
        """
        Get an individual session by its ID.
        """
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
        """
        Retrieve all attempts run for a given session ID.
        """
        response = self._get(
            self._make_url(
                "sessions",
                id,
                "attempts",
            )
        )
        return SessionAttempts.model_validate(response.json())

    def get_attempts(self) -> Attempts:
        """
        Retrieve the last 100 attempts for all workflows for all projects.
        """
        response = self._get(
            self._make_url(
                "/attempts",
            )
        )
        return Attempts.model_validate(response.json())

    def get_attempt(
        self,
        id: str,
    ) -> WorkflowAttempt:
        """
        Retrieve an attempt by its ID.
        """
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
        """
        Start an attempt using an instance of AttemptParameters.
        """
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
        project: WorkflowProject,
        revision: str | None = None,
        schedule_from: str | None = None,
    ) -> Project:
        """
        Upload an entire Digdag project reference tagged to a specific revision. The project will be compiled
        into a zip archive and uploaded to the Digdag host.

        If a revision is not proided, a UUIDv4 value will be generated in its place.

        Optionally, start scheduling workflows for the revision from a provided ISO8601 timestamp.
        """
        archiver = ProjectArchiver()
        revision = revision or str(uuid.uuid4())
        tarball_content = archiver.archive(project)
        return self._upload_project_archive(
            tarball_content,
            project.name,
            revision,
            schedule_from,
        )

    def delete_project(
        self,
        id: str,
    ) -> Project:
        """
        Delete a project by its ID. This only deletes the project archives and workflows but preserves
        any session history.
        """
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
        """
        Retrieve all projects.

        Optionally, if a name is provided, retrieve specifically the project name requested. A collection is still
        returned even when a name is provided.
        """
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
        """
        Retrieve an individual project by ID.
        """
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
        """
        Retrieve a project by name.

        This is a shortcut method that calls the full colllection of projects and filters by the provided name.
        """
        return self.get_projects().filter_by_name(name)

    def get_project_workflows(
        self,
        id: str,
    ) -> Workflows:
        """
        Retrieve all workflows for a project by project ID.
        """
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
        """
        Retrieve a specific workflow from a project by project name and workflow name.
        """
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
        """
        Retrieve all revisions of a project by project ID.
        """
        response = self._get(
            self._make_url(
                "projects",
                id,
                "revisions",
            )
        )
        return ProjectRevisions.model_validate(response.json())
