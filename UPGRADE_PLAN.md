# EWAH Upgrade Plan: Getting off Python 3.10

**Linear issue:** [PLT-25](https://linear.app/gemma-analytics/issue/PLT-25/plan-ewah-upgrade-python-310-eol-oct-31-2026)
**Status:** Proposal (July 2026)

## Why

Python 3.10 reaches end of life on October 31, 2026. After that date it receives no security fixes and disappears from managed environments (including Colab Enterprise). EWAH currently runs on Python 3.10 because its Docker base image is `apache/airflow:2.3.4-python3.10`, and Airflow 2.3.4 supports nothing newer than Python 3.10.

The deeper problem: Apache Airflow 2 itself reached end of life on April 22, 2026. The last 2.x release is 2.11.2 (March 2026). There will be no further security patches for any Airflow 2 version. So "get off Python 3.10" is really an Airflow upgrade project, and even the newest Airflow 2 release only buys time rather than restoring vendor support.

### Version constraints (verified July 2026)

| Fact | Detail |
|---|---|
| Python 3.10 EOL | Oct 31, 2026 |
| Python 3.11 EOL | Oct 31, 2027 |
| Python 3.12 EOL | Oct 31, 2028 |
| Python 3.13 EOL | Oct 31, 2029 (requires Airflow 3.1+) |
| First Airflow with Python 3.11 support | 2.7.0 |
| First Airflow with Python 3.12 support | 2.10.0 |
| First Airflow with Python 3.13 support | 3.1.0 |
| Airflow 2 EOL | April 22, 2026 (last release: 2.11.2) |
| Airflow 3 current stable | 3.3.0 (July 2026); community guidance: enter at 3.1+, not 3.0 |
| Recommended migration path | 2.3.4 → 2.11.x → 3.1+ (direct 2.3 → 3.x jumps are discouraged) |

## Recommendation

Split the work into two separately shipped phases, preceded by a safety-net phase:

- **Phase 0 — Safety net.** Freeze current dependency state and add CI smoke tests. Without this, we have no way to tell whether an upgrade broke anything: the repo has no test suite, no linter, and no CI checks beyond an LLM-based PR review.
- **Phase 1 — Airflow 2.11.2 on Python 3.12.** This solves the Python 3.10 EOL problem before the October deadline with moderate, well-understood effort. Target: ship by end of September 2026.
- **Phase 2 — Airflow 3.1+ (likely 3.3.x by then).** Restores security support and unlocks Python 3.13. Larger effort with one genuine design problem (see below). Target: first half of 2027, tracked as its own Linear issue.

Why Python 3.12 and not 3.13: Python 3.13 requires Airflow 3.1+, which would force both migrations into a single step. Python 3.12 is supported by Airflow 2.10+, all Airflow 3.x releases, and official Airflow constraint files, and its EOL (Oct 2028) leaves comfortable runway. It works for both phases without another rebase.

Why not jump straight to Airflow 3: the official upgrade tooling (`airflow config lint`, sequential DB migrations, deprecation warnings) assumes you pass through late 2.x. A combined jump makes rollback impossible and stacks two failure domains (Python/dependency churn and Airflow architectural changes) into one release.

---

## Current state inventory

### Where versions are pinned

| Location | Pin |
|---|---|
| `Dockerfile:1` | `FROM apache/airflow:2.3.4-python3.10` — the single source of both the Airflow and Python versions |
| `setup.py:23` | `python_requires=">=3.6"` (stale, does not reflect reality) |
| `setup.py` | No `apache-airflow` dependency declared at all; EWAH relies entirely on the base image |
| `.github/workflows/new_release.yml:31` | Release job builds the package with Python 3.8 |
| `.github/workflows/claude-review.yml:19-20` | LLM review instructions enforce "Airflow 2.3.x / Python 3.10 compatibility" |
| `CLAUDE.md` | States Python 3.10 / Airflow 2.3.x constraints for code assistants |

### Airflow API usage that breaks or is deprecated

| Code | Problem | Breaks in |
|---|---|---|
| `ewah/utils/dbt_operator.py:19` — `from distutils.dir_util import copy_tree` | `distutils` removed from the standard library | **Python 3.12** (hard break) |
| `ewah/uploaders/google_sheets.py:4,54,58`, `ewah/utils/airflow_utils.py:2` — `DummyOperator` | Deprecated since Airflow 2.3 in favor of `EmptyOperator`; removed in Airflow 3 | Airflow 3 (warning on 2.11) |
| `airflow/dags/log_cleanup.py:1` — `airflow.operators.bash_operator` | Pre-2.0 import path | Already removed in later 2.x |
| All four DAG factories — `DAG(schedule_interval=...)` | Renamed to `schedule` in 2.4 | Airflow 3 (warning on 2.11) |
| `ewah/utils/airflow_utils.py:15` — `EWAHSqlSensor(SqlSensor)` from `airflow.sensors.sql` | Sensor moved to the `common.sql` provider; subclass overrides the private `_get_hook()` method | Import path deprecated in 2.x; private API may change |
| `ewah/utils/airflow_utils.py:50` — `datetime.utcnow()` (central helper, plus direct calls in `amazon_seller_central`, `plentymarkets` hooks/operators) | Deprecated in Python 3.12 | Warning only, removal in a future Python |
| `ewah/utils/yml_loader.py:10,27` — `airflow.utils.db.create_session` at DAG-parse time | Direct metadata-DB access from parsing/workers removed (AIP-72) | **Airflow 3** (hard break) |
| `docker/scripts/entrypoint_prod.py:14` — raw `settings.Session()` to seed connections | Same direct-DB-access removal; also fragile | Airflow 3 |
| `airflow.operators.bash/python/email` imports (DAG factories, utils) | Core operators move to the `apache-airflow-providers-standard` package | Airflow 3 (extra dependency) |

Confirmed non-issues: no `execution_date` context-key usage (the code already uses `data_interval_start`/`data_interval_end`), no `@apply_defaults`, no timetables, no Airflow plugins, no FAB UI customization. The `apache_airflow_provider` entry-point mechanism (`setup.cfg` → `ewah/utils/airflow_provider_info.py`) is unchanged in shape through Airflow 3.

### Dependency risks

Everything below currently resolves against whatever the 2.3.4 base image happens to allow. The upgrade re-resolves all of it at once.

| Dependency | Issue |
|---|---|
| `sqlalchemy`, `snowflake-sqlalchemy` | Fully unpinned; Airflow 2.11 needs SQLAlchemy 1.4.36+, Airflow 3 uses SQLAlchemy 2.x |
| `pandas`, `pyarrow`, `protobuf`, `snowflake-connector-python` | Fully unpinned; mutual constraints resolved by luck today. `PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION=python` workaround in `Dockerfile:122` suggests past pain |
| `pymssql==2.3.1` | Exact pin because 2.3.2 failed to build; recheck whether upstream fixed the wheel |
| `certifi==2025.1.31` | Exact pin as a Snowflake certificate workaround; will go stale, recheck under new stack |
| `oauth2client` | Deprecated by Google years ago; replace with `google-auth` or drop |
| `setuptools<71` pin (`Dockerfile:83`) + `python setup.py sdist bdist_wheel` (`new_release.yml`) | Legacy packaging flow; migrate to `pyproject.toml` + `python -m build` |
| Chromedriver install (`Dockerfile:47`) | Uses the retired `chromedriver.storage.googleapis.com` LATEST_RELEASE endpoint; must move to Chrome for Testing endpoints (or drop Selenium support) |
| Oracle Instant Client 19.8 (`Dockerfile:63`), `cx_Oracle` extra | amd64-only, hardcoded download URL; decide whether Oracle support is still needed |
| `yahoofinancials-gemma-analytics==1.23` | In-house fork; verify it installs and works on Python 3.12 |

### Testing gap

There is no test suite, linter, or CI verification of any kind (confirmed by repo-wide search). The only automated review is the Claude-based PR reviewer, which checks compatibility textually. Every change below must therefore be validated by the smoke tests introduced in Phase 0 plus manual runs of representative DAGs.

---

## Phase 0 — Safety net (~2–3 days)

1. **Freeze current state.** Run `pip freeze` inside the current working image and commit the output as `constraints-current.txt`. This makes dependency diffs attributable when the resolver re-runs under the new stack.
2. **Add a CI smoke-test workflow** (new `.github/workflows/ci.yml`), running on every PR:
   - Build the `dev_build` Docker stage.
   - Import every module under `ewah/` (catches syntax/import errors across all 40+ connectors, including optional-dependency issues).
   - Run `airflow dags list` (or a DagBag load) against the example `airflow/dags/dags.yml` with a local Postgres, asserting zero import errors.
3. **Document the rollout runbook** per deployment: metadata-DB backup command, image rollback procedure. The notes in `ENV/TODO.md` from the 2.2 → 2.3.4 upgrade (the `_airflow_moved__2_2__task_instance` table gotcha) are prior art.

## Phase 1 — Airflow 2.11.2 + Python 3.12 (est. 1–2 weeks + staged rollout)

### 1.1 Base image and install

- `Dockerfile:1`: `FROM apache/airflow:2.11.2-python3.12` (both stages).
- Install EWAH with the official constraints file for that combination: `https://raw.githubusercontent.com/apache/airflow/constraints-2.11.2/constraints-3.12.txt`.
- Add `apache-airflow-providers-common-sql` explicitly (needed for the SqlSensor move; it ships in the base image but should be declared).

### 1.2 Code changes (all required or strongly advised for this hop)

| File | Change |
|---|---|
| `ewah/utils/dbt_operator.py:19` | Replace `distutils.dir_util.copy_tree(a, b)` with `shutil.copytree(a, b, dirs_exist_ok=True)` |
| `ewah/utils/airflow_utils.py:2`, `ewah/uploaders/google_sheets.py:4,54,58` | `DummyOperator` → `EmptyOperator` (`airflow.operators.empty`) |
| `ewah/utils/airflow_utils.py:15-24` | Re-base `EWAHSqlSensor` on `airflow.providers.common.sql.sensors.sql.SqlSensor`; re-verify the `_get_hook()` override against the 2.11 implementation (it is a private method and its signature changed across versions) |
| `ewah/dag_factories/dag_factory_atomic.py`, `dag_factory_idempotent.py`, `dag_factory_mixed.py`, `dbt_dag_factory.py` | `DAG(schedule_interval=...)` → `DAG(schedule=...)` |
| `airflow/dags/log_cleanup.py:1` | `airflow.operators.bash_operator` → `airflow.operators.bash` |
| `ewah/utils/airflow_utils.py:50` and direct callers in `ewah/hooks/amazon_seller_central.py`, `ewah/hooks/plentymarkets.py`, `ewah/operators/amazon_seller_central.py` | `datetime.utcnow()` → `datetime.now(timezone.utc)`; the central `datetime_utcnow_with_tz()` helper covers most call sites |
| `docker/scripts/entrypoint_prod.py` | Keep as is for Phase 1 (still works on 2.11), but add a TODO referencing Phase 2 |

Run the Airflow 3 lint early for visibility, even though fixes are Phase 2 scope: `ruff check --preview --select AIR301,AIR302,AIR303 ewah/`.

### 1.3 Dependency reconciliation

- Re-resolve all dependencies under the 2.11.2/py3.12 constraints; fix conflicts as they surface (expect friction around `snowflake-connector-python`/`pyarrow`/`pandas` and `google-ads`).
- Replace `oauth2client` usage with `google-auth`, or vendor the minimal piece used.
- Attempt to lift `pymssql==2.3.1` and `certifi==2025.1.31`; keep with updated comments if still needed.
- Rework the Chrome/Chromedriver install to the Chrome for Testing JSON endpoints, or decide to drop Selenium-based connectors (check with users of those connectors first).
- Confirm whether Oracle support (`ewah[oracle]`, Instant Client 19.8) still has users; if not, drop it and simplify the Dockerfile.

### 1.4 Packaging and repo housekeeping

- `setup.py`: set `python_requires=">=3.12"`, add explicit `apache-airflow>=2.10,<3.0` to `install_requires`, add proper 3.12 classifiers. Preferably migrate `setup.py`/`setup.cfg` to `pyproject.toml` (keeping the `apache_airflow_provider` entry point) and drop the `setuptools<71` pin.
- `new_release.yml`: bump the build job to Python 3.12 and switch `python setup.py sdist bdist_wheel` to `python -m build`.
- Update `CLAUDE.md` and `claude-review.yml` so assistants and reviews enforce the new versions (Airflow 2.11 / Python 3.12), not the old ones.
- Delete stale build artifacts (`build/`, `dist/`, `ewah.egg-info/` reflect version 0.9.7rc4) and gitignore them.
- Bump the minor version: `0.9.x` → `0.10.0` (breaking runtime change for all deployments).

### 1.5 Metadata DB migration and rollout

1. Before upgrading any deployment, clean out old XCom entries, task instances, and logs; smaller tables migrate much faster.
2. Back up the metadata DB.
3. Deploy the new image to one dev/internal deployment; `airflow db migrate` walks the schema from 2.3.4 to 2.11.2 sequentially.
4. Soak for at least a full scheduling cycle of every DAG pattern (atomic, idempotent with backfill, mixed) plus a dbt DAG.
5. Roll out deployment by deployment. Rollback = restore DB backup + previous image tag (schema migrations are not reversible in place).

### 1.6 Verification

- CI smoke tests from Phase 0 pass on the new image.
- `docker-compose up` locally: UI reachable, all example DAGs parse without import errors.
- Run one connector per hook family end to end against a sandbox source where feasible; at minimum run the highest-traffic connectors (Postgres, Google Sheets, Shopify, Snowflake/BigQuery/Postgres uploaders).
- Confirm connection forms render in the UI for a sample of `ewah_*` connection types (the custom-connection UI hook interface changed across 2.x).

## Phase 2 — Airflow 3.1+ (separate issue, est. 3–4+ weeks, first half of 2027)

Scope sketch; to be planned in detail once Phase 1 has soaked.

1. **`ewah/utils/yml_loader.py` redesign (the one real design problem).** The YAML loader queries all Airflow Variables via a raw DB session at DAG-parse time and injects them into the Jinja context of `dags.yml`. Airflow 3 removes direct DB access from parsing and workers (AIP-72). Options to evaluate:
   - Read config from environment variables / a secrets backend instead of Airflow Variables.
   - Fetch Variables through the Task SDK / API path where supported.
   - Pre-render `dags.yml` outside the DAG processor (e.g., at deploy time).
2. `docker/scripts/entrypoint_prod.py`: replace raw-session connection seeding with `airflow connections import` (it accepts JSON/YAML natively; keep the Jinja pre-rendering step if needed).
3. Add `apache-airflow-providers-standard` for `BashOperator`/`PythonOperator`/`EmailOperator`/`EmptyOperator`.
4. Run `ruff check --preview --select AIR301,AIR302,AIR303 --fix` and work through the remainder by hand.
5. Adapt deployment: webserver → API server split, `airflow config lint`/`airflow config update` on all `AIRFLOW__*` env vars in the Dockerfile and deployments, `catchup_by_default` now `False` (EWAH's DAG factories set `catchup` explicitly — verify), REST API v2 for anything calling the API.
6. Re-verify the provider entry point and connection-form rendering under Airflow 3.
7. Optionally bump to Python 3.13 in the same pass (supported from Airflow 3.1).

## Timeline

| When | Milestone |
|---|---|
| Aug 2026 | Phase 0 merged (CI smoke tests, constraints freeze) |
| Aug–Sep 2026 | Phase 1 development and internal soak |
| Sep 2026 | Phase 1 rollout to all deployments, before the Oct 31 Python 3.10 EOL |
| Q1–Q2 2027 | Phase 2 (Airflow 3) planned and executed as its own issue |

## Open questions

1. Are the Selenium-based connectors and the Oracle extra still in use anywhere? Dropping them removes the two ugliest parts of the Dockerfile.
2. Which deployments consume `gemmaanalytics/ewah:latest` vs. pinned tags? `latest` consumers will pick up the breaking image automatically on their next pull; they may need pinning before Phase 1 ships.
3. Does anything outside this repo (deployment repos, Colab notebooks) import `ewah` directly and therefore care about `python_requires`?

## Sources

- Airflow supported versions and EOL: https://airflow.apache.org/docs/apache-airflow/stable/installation/supported-versions.html
- Airflow 2 EOL announcement: https://www.astronomer.io/airflow-2-eol/
- Airflow 3 upgrade guide: https://airflow.apache.org/docs/apache-airflow/stable/installation/upgrading_to_airflow3.html
- Airflow 3 breaking changes tracker: https://cwiki.apache.org/confluence/display/AIRFLOW/Airflow+3+breaking+changes
- Migration checklist (2.11 → 3.1+ path, ruff tooling): https://www.astronomer.io/blog/upgrading-airflow-2-to-airflow-3-a-checklist-for-2026/
- Python EOL schedule: https://devguide.python.org/versions/
- Per-version Python support verified against PyPI classifiers (`pypi.org/pypi/apache-airflow/<version>/json`)
