# Contributing to ml-forecasting-incubator

Thanks for taking the time to contribute to ml-forecasting-incubator!

## Setting up a development environment

This project is written in Python and it's dependencies are managed with
[uv](https://github.com/astral-sh/uv). We recommend you install this in your system
environment or within a Python virtual environment for the best experience.

In addition to the project's core dependencies, this project also uses several
open-source tools for linting, formatting, and testing.

uv is used for managing virtual environments. We recommend using it to sync your
development environment with the project's dependencies.

After installing uv, you can create a virtual environment for the project by
running:

```bash
uv venv --python 3.11
source .venv/bin/activate
uv sync --all-extras
```

When you're preparing to open a merge request, ensure your code is formatted properly
by using ruff or pre-commit. These will be available to you if you installed the extra
dependencies with uv.

```bash
ruff check --fix
ruff format
```

or

```bash
pre-commit run --all-files
```

You can also install pre-commit on the repository by running:

```bash
pre-commit install
```

You will also want to ensure your code is covered by tests and that all tests pass.
This requires that you have a Snowflake account available with the default
configuration. Learn more at
[Connecting using the connections.toml file](https://docs.snowflake.com/en/developer-guide/python-connector/python-connector-connect#connecting-using-the-connections-toml-file).
You can run the tests with:

```bash
pytest
```

### Ways to Contribute

There are many ways to contribute, including:

- Reporting Issues: If you find a bug or have a feature request, please open an issue.
- Submitting Pull Requests: If you'd like to contribute code, please fork the
repository and submit a merge request.
- Improving Documentation: If you'd like to help improve our documentation, please
submit a merge request with your changes.

### When submitting a pull request, please make sure to:

- Include a clear and concise description of the changes made.
- Include relevant tests for any new functionality.
- Ensure that the code is properly formatted and linted.

### Code Review

All merge requests will be reviewed by the project maintainers. We will do our best to
review merge requests in a timely manner, but please be patient as we may not always be
able to review them immediately.
