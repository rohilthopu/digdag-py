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
