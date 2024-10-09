# Digdag Workflow and API Client Python SDK

`digdag-py` is a low dependency wrapper over many of the constructs and functionalities provided through [Digdag](https://www.digdag.io/).

`digdag-py` only relies on two libraries, `Pydantic` and `requests` to abstract the concepts and directives used in `.dig` files into a code-native friendly way.

By prioritizing workflow definition generating in code, it is much easier to generate dynamic workflows using native Python through things like for-loops, list manipulation, and more.

Additionally, `digdag-py` provides a simple client for interacting with the Digdag REST API server on your instance.

## Inspiration

I have used Digdag professionally in my work for many years, and although execution on the Digdag platform is good, the developer tooling is definitely lacking.

When you have hundreds of workflow files, all written in YAML, it becomes very difficult to manipulate each individual file when large scale changes occur.

Additionally, when the same basic tasks needed to be executed for hundreds of task combinations, it can be extremely cumbersome writing or copying dig files by hand.
