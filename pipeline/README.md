# pipeline

## Getting started

### Installing dependencies

**Option 1: uv**

Ensure [`uv`](https://docs.astral.sh/uv/) is installed following their [official documentation](https://docs.astral.sh/uv/getting-started/installation/).

From the repository root, create the project environment and install the
dependencies:

```bash
uv sync
```

Then, activate the project environment:

| OS      | Command                     |
| ------- | --------------------------- |
| MacOS   | `source .venv/bin/activate` |
| Windows | `.venv\Scripts\activate`    |

### Running Dagster

Start the Dagster UI web server:

```bash
uv run dg dev
```

Open http://localhost:3000 in your browser to see the project.

## Learn more

To learn more about this template and Dagster in general:

- [Dagster Documentation](https://docs.dagster.io/)
- [Dagster University](https://courses.dagster.io/)
- [Dagster Slack Community](https://dagster.io/slack)
