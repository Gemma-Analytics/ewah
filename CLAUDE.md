# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What is EWAH

EWAH (ELT With Airflow Helper) is an Apache Airflow 2.3.4 provider package that extracts data from 40+ sources and loads it into PostgreSQL, Snowflake, or BigQuery. It runs on Python 3.10. Strictly ELT — no transformations beyond what's needed for relational format.

## Development Commands

```bash
# Start dev environment (Airflow UI at http://localhost:8084, user: ewah, pass: ewah)
docker-compose up

# Install from source (inside container or locally)
pip install -e .

# Build distribution
python setup.py sdist bdist_wheel
```

There is no test suite, no linter, and no formatter configured.

## Architecture

### Component Hierarchy

```
YAML Config (dags.yml)
  → DAG Factory (dag_factories/)  — creates Airflow DAGs
    → Operator (operators/)       — extraction orchestration + parameters
      → Hook (hooks/)             — connection + API/DB communication
        → Uploader (uploaders/)   — loads into target DWH
```

### Adding a New Connector

Each connector requires two files:

1. **Operator** at `ewah/operators/<source>.py` — must inherit `EWAHBaseOperator` and implement:
   - `_NAMES`: list of YAML aliases (e.g., `["shopify"]`)
   - `_ACCEPTED_EXTRACT_STRATEGIES`: dict of supported strategies
   - `ewah_execute(self, context)`: core extraction logic that calls `self.upload_data(batch)`

2. **Hook** at `ewah/hooks/<source>.py` — must inherit `EWAHBaseHook` and define:
   - `_ATTR_RELABEL`: maps custom attributes to Airflow connection fields
   - `conn_type`: connection type string (e.g., `"ewah_shopify"`)
   - Data fetching method(s) like `get_data()` or `get_data_in_batches()`

Both directories auto-discover classes at import time — no registration step needed.

### Extract Strategies

| Strategy | Constant | Behavior |
|----------|----------|----------|
| `full-refresh` | `EC.ES_FULL_REFRESH` | Reload all data every run |
| `incremental` | `EC.ES_INCREMENTAL` | Load data within `data_interval_start` to `data_interval_end` |
| `subsequent` | `EC.ES_SUBSEQUENT` | Load data newer than max value of `subsequent_field` |

### DAG Factory Patterns

| Pattern | Function | Returns | Use case |
|---------|----------|---------|----------|
| Atomic | `dag_factory_atomic` | 1 DAG | Full refresh / subsequent loads |
| Idempotent | `dag_factory_idempotent` | 3 DAGs (reset, backfill, current) | Incremental with historical backfill |
| Mixed | `dag_factory_mixed` | 2 DAGs (full refresh, incremental) | Frequent incremental + periodic full refresh |

### Schema Isolation

Each source loads into its own schema (e.g., `raw_shopify`). Data loads into a temporary `{schema}_next` schema, then atomically swaps on success. Original data is preserved on failure.

## Version Constraints

- **Airflow 2.3.x**: Use `context["data_interval_start"]` / `context["data_interval_end"]`, not deprecated `context["execution_date"]`. Do not use features from Airflow 2.4+ (setup/teardown tasks, datasets as triggers, `airflow.sdk` imports).
- **Python 3.10**: Do not use features from 3.11+ (ExceptionGroup, TaskGroup) or 3.12+ (type statement).

## Key Files

- `VERSION` — single source of truth for package version
- `EWAH_ARCHITECTURE.md` — detailed architecture reference with connector walkthrough
- `airflow/dags/dags.yml` — example DAG configurations
- `ewah/constants/__init__.py` — all constants (DWH engines, strategies, type mappings)
- `ewah/operators/base.py` — EWAHBaseOperator (core execution flow, schema management, metadata)
- `ewah/hooks/base.py` — EWAHBaseHook, EWAHConnection (connection auto-discovery)
- `ewah/utils/yml_loader.py` — YAML config parser with Jinja2 templating support

## Workflow

- All changes must go through a pull request — never commit directly to master.
- When a change needs to be deployed, bump the patch version in `VERSION` (e.g., `0.9.23` → `0.9.24`) as part of the PR. Only bump minor or major versions when explicitly requested.
- Merging to master with a changed `VERSION` file triggers the `new_release.yml` workflow, which creates a GitHub release, publishes to PyPI, and builds multi-arch Docker images (amd64 + arm64).
