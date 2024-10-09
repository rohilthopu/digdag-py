# Digdag Workflow and API Client Python SDK

`digdag-py` is a low dependency wrapper over many of the constructs and functionalities provided through [Digdag](https://www.digdag.io/).

`digdag-py` only relies on two libraries, `Pydantic` and `requests`, to abstract the concepts and directives used in `.dig` files into a code-native friendly way.

By prioritizing workflow definition generating in code, it is much easier to generate dynamic workflows using native Python through things like for-loops, list manipulation, and more.

Additionally, `digdag-py` provides a simple client for interacting with the Digdag REST API server on your instance.

## Inspiration

I have used Digdag professionally in my work for many years, and although execution on the Digdag platform is good, the developer tooling is definitely lacking.

When you have hundreds of workflow files, all written in YAML, it becomes very difficult to manipulate each individual file when large scale changes occur.

Additionally, when the same basic tasks needed to be executed for hundreds of task combinations, it can be extremely cumbersome writing or copying dig files by hand.

## Overview

`digdag-py` attempts to provide the following:

1. A simple Digdag REST API client
2. Pydantic domain models mapped to each type of API response
3. Pydantic models that abstract Project, Workflow, and Task configurations in Digdag
4. Tools to export project archives and workflows that can be uploaded to a Digdag instance through the REST API client.

## Basic Workflow Example

Workflows in `digdag-py` can be defined using the models found in the `python digdagpy.dig.models` module.

Workflows have two basic components:

1. A workflow name
2. A list of tasks to be executed

A basic definition can be seen below.

```python
from digdagpy.dig.models import Workflow, CommandTask

workflow = Workflow(
    name="task-to-be-executed",
    tasks=[
        CommandTask(
            name="print-hello",
            command="echo hello",
        )
    ]
)
```

The defined `workflow` has a single task that will be executed that prints "hello" to the terminal.

In order to see the generated `.dig` file content from this workflow, we can use the `export_workflow` function provided in `python digdagpy.dig.dig`.

```python

from digdagpy.dig.models import Workflow, CommandTask

from digdagpy.dig.dig import export_workflow

workflow = Workflow(
    name="task-to-be-executed",
    tasks=[
        CommandTask(
            name="print-hello",
            command="echo hello",
        )
    ]
)

dig_content: str = export_workflow(
    workflow,
)

print(dig_content)
```

This will generate a YAML output as follows:

```yaml
timezone: UTC

+print-hello:
    sh>: echo hello
```

## Complex Example

The following example uses both for loops and conditional task creation/manipulation to generate a group of workflows that execute one hour apart, sequentially.

```python
from digdagpy.dig.models import Workflow, WorkflowSchedule, CommandTask, ErrorTask


tasks = (
    "first_task",
    "second_special_task",
    "third_task",
)


for hour_offset, task_name in enumerate(tasks):
    start_hour_adjusted = 24 - len(tasks) + hour_offset - 1

    schedule = WorkflowSchedule(
        daily="{}:00:00".format(start_hour_adjusted),
        skip_delayed_by=1,
    )

    if task_name == "second_special_task":
        tasks: list[NamedTask] = [
            CommandTask(
                name="process-special-task",
                command=f"python main.py create {task_name} --save",
            )
        ]

        upload_task = NamedTask(name="upload")

        for destination in (
            "int",
            "stg",
            "prod",
        ):
            upload_task.add_task(
                NamedTask(
                    name=destination,
                    tasks=[
                        CommandTask(
                            name="upload",
                            command=f"python main.py upload {task_name} --source-env prod --destination-env {destination}",
                        ),
                        CommandTask(
                            name="implicit-special-task",
                            command=f"python main.py implicit-task --source-env prod --destination-env {destination}",
                        ),
                    ],
                )
            )

        tasks.append(upload_task)
    else:
        tasks: list[NamedTask] = [
            CommandTask(
                name="create",
                command=f"python main.py create {task_name} --save",
            ),
            NamedTask(
                name="upload",
                tasks=[
                    CommandTask(
                        name=destination,
                        command=f"python main.py upload {task_name} --source-env prod --destination-env {destination}",
                    )
                    for destination in (
                        "int",
                        "stg",
                        "prod",
                    )
                ],
            ),
        ]


    tasks.insert(
        0,
        CommandTask(
            name="lock-deployment",
            command=f"python main.py lock-deployment {task_name}",
        ),
    )

    tasks.append(
        CommandTask(
            name="unlock-deployment",
            command=f"python main.py unlock-deployment {task_name}",
        ),
    )

    project.add_workflow(
        Workflow(
            name=f"process-task-{task_name}",
            tasks=tasks,
            schedule=schedule,
            error=ErrorTask(
                command=f"python main.py unlock-deployment {task_name}",
            ),
        )
    )
```

This will generate tasks as follows:

### first_task

```yaml
timezone: UTC

schedule:
    daily>: 20:00:00
    skip_delayed_by: 1s
    skip_on_overtime: true

+lock-deployment:
    sh>: python main.py lock-deployment first_task

+create:
    sh>: python main.py create first_task --save

+upload:
    +int:
        sh>: python main.py upload first_task --source-env prod --destination-env int

    +stg:
        sh>: python main.py upload first_task --source-env prod --destination-env stg

    +prod:
        sh>: python main.py upload first_task --source-env prod --destination-env prod

+unlock-deployment:
    sh>: python main.py unlock-deployment first_task

_error:
    sh>: python main.py unlock-deployment first_task
```

### second_special_task

Notice that with this task, there was a special case in the workflow generation that added some extra functionality only for this task.

```yaml
timezone: UTC

schedule:
    daily>: 21:00:00
    skip_delayed_by: 1s
    skip_on_overtime: true

+lock-deployment:
    sh>: python main.py lock-deployment second_special_task

+process-special-task:
    sh>: python main.py create second_special_task --save

+upload:
    +int:
        +upload:
            sh>: python main.py upload second_special_task --source-env prod --destination-env int

        +implicit-special-task:
            sh>: python main.py implicit-task --source-env prod --destination-env int

    +stg:
        +upload:
            sh>: python main.py upload second_special_task --source-env prod --destination-env stg

        +implicit-special-task:
            sh>: python main.py implicit-task --source-env prod --destination-env stg

    +prod:
        +upload:
            sh>: python main.py upload second_special_task --source-env prod --destination-env prod

        +implicit-special-task:
            sh>: python main.py implicit-task --source-env prod --destination-env prod

+unlock-deployment:
    sh>: python main.py unlock-deployment second_special_task

_error:
    sh>: python main.py unlock-deployment second_special_task
```

### third_task

This task is identical to the first.

```yaml
timezone: UTC

schedule:
    daily>: 22:00:00
    skip_delayed_by: 1s
    skip_on_overtime: true

+lock-deployment:
    sh>: python main.py lock-deployment third_task

+create:
    sh>: python main.py create third_task --save

+upload:
    +int:
        sh>: python main.py upload third_task --source-env prod --destination-env int

    +stg:
        sh>: python main.py upload third_task --source-env prod --destination-env stg

    +prod:
        sh>: python main.py upload third_task --source-env prod --destination-env prod

+unlock-deployment:
    sh>: python main.py unlock-deployment third_task

_error:
    sh>: python main.py unlock-deployment third_task
```

Notice how the three tasks all run exactly one hour apart, as calculated using a for loop in Python.

## REST API Client

`digdag-py` also provides a simple REST API client to interact with the Digdag API.

Some features, such as starting attempts, are also abstracted over to make for a convenient experience.

## Creating and Starting Attempts

Starting an attempt of an existing workflow using `digdag-py` is simple. This is helpful when triggering jobs from API calls to your services.

The steps to start attempts in Digdag are:

1. Retrieve the project the workflow belongs to
2. Retrieve the workflow from the projects workflows
3. Create an attempt parameters config from the workflow
4. Use the client to start the attempt using the attempt parameters

### Starting a Simple Attempt

```python
from digdag_sdk.api.client import DigdagClient

client = DigdagClient(host="<my_digdag_host>")

project = client.get_projects().filter_by_name("test-project")

workflow = client.get_project_workflows(project.id).filter_by_name("print-hello")

attempt_params = workflow.create_attempt_parameters()

started_attempt = client.start_attempt(attempt_params)
```

### Starting an attempt with parameters

```python
from digdag_sdk.api.client import DigdagClient

client = DigdagClient(host="<my_digdag_host>")

project = client.get_projects().filter_by_name("test-project")

workflow = client.get_project_workflows(project.id).filter_by_name("execute-task")

attempt_params = workflow.create_attempt_parameters(
    params={
        'id': 14,
        'first_name': 'joe',
    }
)

started_attempt = client.start_attempt(attempt_params)
```

Providing a `params` dictionary to the `Workflow.create_attempt_parameters()` method will tell the client to pass the arguments as JSON to the workflow execution.

Note that this only supports simple types that are supported by YAML. More information can be found in the [Digdag docs](https://docs.digdag.io/workflow_definition.html#using-api)
