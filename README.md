# Digdag Workflow and API Client Python SDK

`digdag-py` is a low dependency wrapper over many of the constructs and functionalities provided through [Digdag](https://www.digdag.io/).

`digdag-py` only relies on two libraries, `Pydantic` and `requests`, to abstract the concepts and directives used in `.dig` files into a code-native friendly way.

By prioritizing workflow definition generating in code, it is much easier to generate dynamic workflows using native Python through things like for-loops, list manipulation, and more.

Additionally, `digdag-py` provides a simple client for interacting with the Digdag REST API server on your instance.

## Inspiration

I have used Digdag professionally in my work for many years, and although execution on the Digdag platform is good, the developer tooling is definitely lacking.

When you have hundreds of workflow files, all written in YAML, it becomes very difficult to manipulate each individual file when large scale changes occur.

Additionally, when the same basic tasks needed to be executed for hundreds of task combinations, it can be extremely cumbersome writing or copying dig files by hand.

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

```
timezone: UTC

+print-hello:
    sh>: echo hello
```

## Complex Example

The following example uses both for loops and conditional task creation/manipulation to generate a group of workflows that execute one hour apart, sequentially.

```python
from digdagpy.dig.models import Workflow, WorkflowSchedule, CommandTask, ErrorTask


feed_options = (
    "first_task",
    "second_special_task",
    "third_task",
)

for hour_offset, task_name in enumerate(feed_options):
    start_hour_adjusted = 24 - len(feed_options) + hour_offset - 1

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