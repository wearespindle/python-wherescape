# Glassfrog connector

Loads governance and tactical meeting data from [Glassfrog](https://www.glassfrog.com/) (Holacracy operating system) into the WhereScape data warehouse. The grain of the load table is one row per meeting; aggregation per period / per circle is deferred to stage / fact tables.

More information: <https://support.glassfrog.com/glassfrog-api>

## Files

- **glassfrog_wrapper.py** — `Glassfrog` API client (Session + Retry adapter, paginated GETs).
- **glassfrog_create_metadata.py** — Host script that drops the target load table and refreshes column metadata in the WhereScape repository. `COLUMNS` here is the source of truth for the load-table shape.
- **glassfrog_load_data.py** — Host script that fetches governance + tactical meetings, enriches them with circle names, and inserts rows into the load table.
- **tests/test_glassfrog.py** — Unit tests (no live API, no DB). Run: `uv run pytest wherescape/connectors/glassfrog`.

## Preparation

### WhereScape parameter

Add a single parameter in WhereScape RED:

- `glassfrog_apikey` — your Glassfrog API key (sent as `X-Auth-Token` on every request).

The connector reads this with `wherescape.read_parameter("glassfrog_apikey")`. There is no Glassfrog "Connection" object in RED, so the usual `WSL_SRCCFG_APIKEY` env var is **not** used.

### Load table

Add one load table in RED. The exact name is up to you; the connector loads everything to `wherescape.load_full_name`. Suggested name (matching the `load_<source>_<table>` convention):

- `load.load_gf_meetings`

Both governance and tactical meetings land in the same table — `meeting_type` distinguishes them so the BI tool can slice as needed.

### Host scripts

Create two Python host scripts in RED:

```python
# python_glassfrog_create_metadata
from wherescape.connectors.glassfrog.glassfrog_create_metadata import glassfrog_create_metadata

glassfrog_create_metadata()
```

```python
# python_glassfrog_load_data
from wherescape.connectors.glassfrog.glassfrog_load_data import glassfrog_load_data

glassfrog_load_data()
```

Attach `python_glassfrog_create_metadata` to the load table once, run it to populate `ws_load_col`, then create the physical table from RED. From then on, attach `python_glassfrog_load_data` and schedule it.

`glassfrog_create_metadata` is **idempotent**: it drops the target table if it exists and then refreshes the metadata rows. Re-run it whenever the column set changes.

## Standalone execution

Both host scripts have a `__main__` block so they can be run outside RED for local development. Create a `ws_env.py` next to the script (based on [ws_env_template.py](../../ws_env_template.py)) with valid DSNs / credentials, then:

```bash
uv run python wherescape/connectors/glassfrog/glassfrog_create_metadata.py
uv run python wherescape/connectors/glassfrog/glassfrog_load_data.py
```

The `__main__` blocks call `setup_env("load_gf_meetings", schema="load")` — adjust the table name to match what you created in RED.

## Loaded columns

Each column ships with a description that lands in the warehouse column comment. Foreign keys into Glassfrog's user table are tagged `[GDPR_MEDIUM]` per the company sensitive-data labelling policy (see [CLAUDE.md](../../../CLAUDE.md) → "Sensitive data labelling").

| Column | Type | Source | Sensitivity |
|---|---|---|---|
| `id` | bigint | Meeting `id` | `[GDPR_LOW]` |
| `circle_id` | bigint | `links.circle` (falls back to top-level `circle_id`) | — |
| `circle_name` | text | Resolved from `/api/v3/circles` | — |
| `meeting_type` | text | `"governance"` or `"tactical"` (which endpoint the row came from) | — |
| `started_at` | timestamp | `started_at` (falls back to `created_at`) | — |
| `ended_at` | timestamp | `ended_at` (nullable) | — |
| `occurred_on` | date | Date prefix of `started_at` — convenient for period grouping | — |
| `facilitator_id` | bigint | `links.facilitator` (nullable) | `[GDPR_MEDIUM]` |
| `secretary_id` | bigint | `links.secretary` (nullable) | `[GDPR_MEDIUM]` |
| `attendee_count` | int | `len(links.attendees)` (`0` when the list is empty or missing) | — |
| `url` | text | Glassfrog app URL to the meeting | `[GDPR_LOW]` |
| `dss_record_source` | text | `https://api.glassfrog.com/api/v3/` | — |
| `dss_load_date` | timestamp | Load timestamp | — |

`COLUMNS` in [glassfrog_create_metadata.py](glassfrog_create_metadata.py) is the source of truth (name → `{type, comment}`) — keep it aligned with `_build_row()` in [glassfrog_load_data.py](glassfrog_load_data.py). The `tests/test_glassfrog.py::TestColumnsConsistency` test guards against drift.

## API reference

- Base URL: `https://api.glassfrog.com/api/v3/`
- Auth header: `X-Auth-Token: <api_key>`
- Endpoints used:
  - `GET /governance_meetings` — paginated via `page` / `per_page`
  - `GET /tactical_meetings` — paginated via `page` / `per_page`
  - `GET /circles` — fetched once per run for circle-name enrichment
- Pagination: the wrapper walks `page=1, 2, …` until an empty page comes back. Default `per_page=100`.
- Retries: 429 / 500 / 502 / 503 / 504 are retried up to 5 times with `backoff_factor=10` (respects `Retry-After`). Configured at the `requests.Session` level; no manual retry loop in the wrapper.

## Adding a new meeting type

If Glassfrog adds another endpoint (e.g. strategy meetings):

1. Add a `get_strategy_meetings()` method to `Glassfrog` mirroring the existing two.
2. Add `[("strategy", m) for m in self.get_strategy_meetings()]` to `get_all_meetings()`.

Nothing else needs to change — the load table already accommodates the new `meeting_type` value.
