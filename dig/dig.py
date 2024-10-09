import pathlib
import shutil
import tempfile
import tarfile

from io import StringIO, TextIOWrapper

from .models import (
    ErrorTask,
    NamedTask,
    CommandTask,
    EmbeddedTask,
    ParallelTask,
    RepeatableTask,
    IntervalRetryCondition,
    SimpleRetryCondition,
    Workflow,
    DependentWorkflow,
    WorkflowProject,
    DockerBuildImageTask,
    DockerImage,
    YamlSupportedType,
)


def _get_indent_width(level: int) -> str:
    return "    " * level


def _export_task_content(
    f: TextIOWrapper,
    task: NamedTask,
    indent_level: int,
    exports: dict[str, YamlSupportedType] | None = None,
    image: DockerImage | None = None,
):
    # Various levels of tab indentation required to properly write the tasks in the
    # correct hierarchies.
    task_indent: str = _get_indent_width(indent_level)
    sub_item_indent: str = _get_indent_width(indent_level + 1)
    sub_sub_item_indent: str = _get_indent_width(indent_level + 2)

    if isinstance(task, ErrorTask):
        f.write("_error:\n")
    else:
        f.write(task_indent + f"+{task.name}:\n")

    # Exports are optional. Locally scoped exports take precedence over the higher
    # scoped ones ex. Workflow level exports.
    if task.exports is not None:
        f.write(sub_item_indent + "_export:\n")
        for k, v in task.exports.items():
            f.write(sub_sub_item_indent + f"{k}: {v}\n")
        f.write("\n")

    if task.retry_condition is not None:
        if isinstance(task.retry_condition, IntervalRetryCondition):
            f.write(sub_item_indent + "_retry: \n")
            f.write(sub_sub_item_indent + f"limit: {task.retry_condition.limit}")
            f.write(sub_sub_item_indent + f"interval: {task.retry_condition.interval}")
            f.write(
                sub_sub_item_indent
                + f"interval_type: {task.retry_condition.interval_type.value}"
            )
        elif isinstance(task.retry_condition, SimpleRetryCondition):
            f.write(sub_item_indent + f"_retry: {task.retry_condition.limit}")

        f.write("\n")

    if isinstance(task, ParallelTask):
        # Parallel tasks have a short hand notation for the parallel flag when no other
        # configuration properties are provided. Opt to just set the option to 'true' when
        # no limit is provided.

        # Additionally, the full configuration object is required to even instantiate a ParallelTask
        # so we can first make the assumption that the intent is for the Task to run in parallel and that
        # if a limit is not provided, then the tasks should ALL run in parallel.
        if task.configuration.limit is not None:
            f.write(sub_item_indent + "_parallel:\n")
            f.write(sub_sub_item_indent + f"limit: {task.configuration.limit}")
        # Although the parallel flag really should just be True, optionally, allow the value to be false
        # as a quick shorthand when using a limit. This is because the limit takes precedence over the flag.
        # However, this leaves a weirdly ambiguous condition here where a limit might not be provided and
        # parallel might be set to false.
        # In that case, the task will be treated like any plain named task, though a warning that a ParallelTask
        # with parallel=False should not be used over a normal NamedTask.s
        elif task.configuration.parallel:
            f.write(sub_item_indent + "_parallel: true\n")
        f.write("\n")

    if isinstance(task, RepeatableTask):
        # RepeatableTask variables are almost identical to exports and thus have the same
        # key, value write-out process.
        f.write(sub_item_indent + "for_each>:\n")
        for k, v in task.iterables.items():
            # Generate a string using the values provided in order to avoid having quotes
            # wrapping each value.
            str_values: str = ", ".join([str(item) for item in v])
            # Inject the values into a literal array hard coded in the string for the line.
            f.write(sub_sub_item_indent + f"{k}: [{str_values}]\n")
        f.write("\n")

    if isinstance(task, CommandTask):
        if image is not None and task.image is None:
            task.image = image

        # Currently fixed order in terms of which action for a command is run first. The workaround
        # here would be to wrap each individual command in a NamedTask to guarantee the order
        # in code.
        if (cmd := task.get_command(exports)) is None:
            raise ValueError("CommandTask was used but no command was provided")

        f.write(sub_item_indent + f"sh>: {cmd}\n")
        f.write("\n")

    if isinstance(task, DependentWorkflow):
        f.write(sub_item_indent + f"require>: {task.workflow}\n")
        if task.project is not None:
            if isinstance(task.project, str):
                f.write(sub_item_indent + f"project_name: {task.project}")
            else:
                # task.project is an int
                f.write(sub_item_indent + f"project_id: {task.project}")

        f.write("\n")

    if isinstance(task, EmbeddedTask):
        f.write(sub_item_indent + f"call>: {task.workflow}\n")
        f.write("\n")


def _export_task(
    f: TextIOWrapper,
    workflow: Workflow,
    task: NamedTask,
    indent_level: int = 0,
    exports: dict[str, YamlSupportedType] | None = None,
    image: DockerImage | None = None,
):
    if exports is None:
        exports = {}

    export_hierarchy: list[dict[str, YamlSupportedType] | None] = [
        workflow.exports,
        task.exports,
    ]

    for eh in export_hierarchy:
        if eh is not None:
            exports.update(eh)

    # Write the task content first before iterating on any subtasks
    _export_task_content(f, task, indent_level, exports, image)

    # This condition is necessary to allow for any subsections, like the _do: section in
    # a repeatable task, to be in included.
    if len(task.tasks) > 0:
        # Repeatable tasks nest their subtasks in a section called _do:, which means that
        # any subtasks need to be temporarily be at another level deep for indentation.
        if isinstance(task, RepeatableTask):
            f.write("    " * (indent_level + 1) + "_do:\n")

            # Bump the indentation for nested tasks to be within the _do:
            indent_level = indent_level + 1

        # Recursively call the export task on each subsequent task
        for subtask in task.tasks:
            _export_task(
                f,
                workflow,
                subtask,
                indent_level=indent_level + 1,
                exports=exports,
                image=image,
            )

        # Undo the previous indentation change so that any subsequent tasks will be at the correct
        # indentation level
        if isinstance(task, RepeatableTask):
            indent_level = indent_level - 1


def export_workflow(workflow: Workflow, image: DockerImage | None = None) -> str:
    f = StringIO()

    f.write(f"timezone: {workflow.timezone}")
    f.write("\n" * 2)
    if workflow.exports is not None:
        f.write("_export:\n")
        for k, v in workflow.exports.items():
            f.write("    " + f"{k}: {v}\n")
        f.write("\n")

    if workflow.schedule is not None:
        f.write("schedule:\n")
        if workflow.schedule.cron is not None:
            f.write("    " + f"cron>: {workflow.schedule.cron.to_string()} \n")
        elif workflow.schedule.daily is not None:
            f.write("    " + f"daily>: {workflow.schedule.daily} \n")
        if (
            workflow.schedule.skip_delayed_by is not None
            and workflow.schedule.skip_delayed_by > 0
        ):
            f.write("    " + f"skip_delayed_by: {workflow.schedule.skip_delayed_by}s\n")
        f.write(
            "    "
            + f"skip_on_overtime: {str(workflow.schedule.skip_on_overtime).lower()}\n"
        )
        f.write("\n")

    for task in workflow.tasks:
        _export_task(f, workflow, task, exports=workflow.exports, image=image)

    if workflow.error is not None:
        workflow.error.name = "_error"
        _export_task(f, workflow, workflow.error, exports=workflow.exports, image=image)

    f.seek(0)
    return f.getvalue()


def _create_dig_archive(project: pathlib.Path) -> bytes:
    with tempfile.TemporaryFile() as temp:
        with tarfile.open(
            mode="w:gz", fileobj=temp, compresslevel=6, format=tarfile.GNU_FORMAT
        ) as tar:
            # for dig_file in project.rglob("**/*.dig"):
            for dig_file in project.iterdir():
                tar.add(dig_file, arcname=dig_file.name)

        # Seek back to the start of the file since we are not closing and re-opening it
        temp.seek(0)
        content = temp.read()
    return content


def create_project_archive(project: WorkflowProject) -> bytes:
    if project.image is not None:
        project.add_workflow(
            Workflow(
                name="build-image",
                tasks=[
                    DockerBuildImageTask(
                        name="build-image",
                        image=project.image,
                    )
                ],
            ),
        )

    with tempfile.TemporaryDirectory() as tmp:
        dir_path = pathlib.Path(tmp)

        # Generate workflow files in the root directory of the temp dir
        for workflow in project.workflows:
            workflow_path = dir_path / (workflow.name + ".dig")

            wf_content = export_workflow(workflow, project.image)
            with workflow_path.open("w") as f:
                f.write(wf_content)

        # Copy overy the entire project contents if a project root folder parent folder is
        # provided. Using this will allow all of the files to be contained within the workflow
        # revision.
        if project.project_root is not None:
            shutil.copytree(
                project.project_root.root_path,
                dir_path,
                ignore=shutil.ignore_patterns(
                    "__pycache__", ".git", *project.project_root.ignore_patterns
                ),
                dirs_exist_ok=True,
            )
        content = _create_dig_archive(dir_path)
    return content
