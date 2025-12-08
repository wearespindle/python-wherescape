# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

This is a Python library for WhereScape RED, a data warehouse automation tool. The library handles reading and writing to the WhereScape repository database (metadata) and target database (data warehouse), with a focus on API connectors that load external data sources into the warehouse.

## Architecture

### Core Components

**wherescape.py** - Main WhereScape class that provides:
- Database connection management via pyodbc (metadata, target, and source databases)
- Environment variable-based configuration (all variables prefixed with `WSL_`)
- Query execution methods: `query_meta()`, `push_to_meta()`, `query_target()`, `push_to_target()`, `query_source()`, `push_to_source()`
- WhereScape parameter management: `read_parameter()`, `write_parameter()`
- Job log management: `update_task_log()`, `job_clear_logs_by_date()`, `job_clear_archive_by_date()`
- Column metadata retrieval: `get_columns()`

**logging.py** - Custom WhereScape logging handler:
- `WhereScapeLogHandler` buffers logs and outputs them with WhereScape-specific exit codes on flush
- Exit codes: `1` (success), `-1` (warnings), `-2` (errors), `-3` (critical)
- Logs to both console (for WhereScape) and rotating file handler (Saturday night rotation)
- Must be initialized via `initialise_wherescape_logging(wherescape_instance)`
- Sets up unhandled exception logging

**helper_functions.py** - Shared utilities:
- `prepare_metadata_query()`: Generates SQL to create/update load table column metadata in WhereScape repository
- `create_column_names()`: Slugifies display names to valid column names (max 59 chars)
- `flatten_json()`: Flattens nested JSON responses from APIs
- `filter_dict()` and `fill_out_empty_keys()`: Clean and normalize API responses

### Connector Architecture

All connectors follow a consistent pattern with three components:

1. **{source}_wrapper.py** - API client wrapper
   - Handles authentication and API requests
   - Returns normalized/flattened data structures

2. **{source}_create_metadata.py** - Metadata creation script
   - Defines expected columns, display names, and data types
   - Uses `prepare_metadata_query()` to generate metadata SQL
   - Creates metadata in WhereScape repository via `push_to_meta()`

3. **{source}_load_data.py** - Data loading script
   - Fetches data from API using wrapper
   - Implements incremental loading using high water marks (WhereScape parameters)
   - Pushes data to target using `push_to_target()` or `push_many_to_target()`
   - Updates task log with row counts via `update_task_log()`

### Available Connectors

- **anythingllm**: Chat history and workspace data from AnythingLLM
- **friday_pulse**: Employee happiness survey data (with lookback period for late responses)
- **gitlab**: Projects, issues, tags, pipelines, merge requests, commits, branches (incremental via high water marks)
- **hubspot**: Companies, contacts, deals, tickets, engagements (supports multiple environments)
- **jira**: Projects and issues (full and incremental loads)

### Validators

**validators/fact_dimension_join.py** - Data quality validation:
- Checks fact-dimension joins in the warehouse
- Counts records with 0-dimension keys (indicating missing dimension data)
- Outputs CSV report to `WSL_WORKDIR`

## Development Environment Setup

### Requirements

Install runtime dependencies:
```bash
pip install -r requirements.txt
```

Install development dependencies:
```bash
pip install -r requirements-dev.txt
```

Required packages:
- pyodbc (database connectivity)
- pandas, numpy (data manipulation)
- requests (API calls)
- hubspot-api-client, notion-client (specific API clients)

Development tools:
- ruff (linting and code formatting)

### WhereScape Environment

The library requires WhereScape environment variables (typically set by WhereScape scheduler). For local development:

1. Copy `wherescape/ws_env_template.py` to `ws_env.py` (in your working directory)
2. Update connection strings, usernames, and passwords
3. Ensure `ws_env.py` is in `.gitignore` (security)
4. Import and call `setup_env()` to simulate WhereScape environment:

```python
from ws_env import setup_env
setup_env("table_name", schema="load", environment="dev1")
```

### Key Environment Variables

All environment variables start with `WSL_` prefix:

**Metadata database (WhereScape repository):**
- `WSL_META_DSN`, `WSL_META_USER`, `WSL_META_PWD`

**Target database (data warehouse):**
- `WSL_TGT_DSN`, `WSL_TGT_USER`, `WSL_TGT_PWD`

**Source database (optional):**
- `WSL_SRC_DSN`, `WSL_SRC_USER`, `WSL_SRC_PWD`

**Job context:**
- `WSL_SEQUENCE`, `WSL_JOB_KEY`, `WSL_JOB_NAME`, `WSL_TASK_KEY`, `WSL_TASK_NAME`
- `WSL_LOAD_TABLE`, `WSL_LOAD_SCHEMA`, `WSL_LOAD_FULLNAME`
- `WSL_WORKDIR` (working directory for logs and output files)

**API source configuration:**
- `WSL_SRCCFG_URL`, `WSL_SRCCFG_USER`, `WSL_SRCCFG_APIKEY`

### Running Tests

There is no formal test suite. The `test.py` file in the root can be used for ad-hoc testing with a local environment setup.

### Code Formatting and Linting

The project uses Ruff for linting and code formatting (configured in [pyproject.toml](pyproject.toml)).

Run linting:
```bash
ruff check .
```

Run formatting:
```bash
ruff format .
```

Configuration details:
- Target: Python 3.12
- Line length: 119 characters
- Enabled rules: pycodestyle (E/W), pyflakes (F), isort (I), pep8-naming (N), flake8-bugbear (B), flake8-comprehensions (C4), flake8-simplify (SIM), pyupgrade (UP)
- See [pyproject.toml](pyproject.toml) for complete configuration

## Working with Connectors

### Creating a New Connector

When adding a new API connector, follow the established pattern:

1. **Create connector directory**: `wherescape/connectors/{source_name}/`

2. **Create wrapper** (`{source}_wrapper.py`):
   - Implement API client class
   - Handle authentication (typically via bearer token or API key)
   - Create methods that return flattened, normalized data
   - Use `helper_functions.flatten_json()` for nested responses

3. **Create metadata script** (`{source}_create_metadata.py`):
   - Define `EXPECTED_COLUMNS` list matching wrapper output
   - Define display names and data types
   - Use `prepare_metadata_query()` to generate SQL
   - Note: `dss_record_source` and `dss_load_date` are added automatically

4. **Create load data script** (`{source}_load_data.py`):
   - Initialize WhereScape instance (logging is automatic)
   - Read high water mark parameter: `wherescape.read_parameter('HWM_{table_name}')`
   - Fetch data from API wrapper with incremental filtering
   - Push data: `wherescape.push_many_to_target(sql, data_rows)`
   - Update task log: `wherescape.update_task_log(inserted=row_count)`
   - Set main message: `wherescape.main_message = "Loaded X records"`
   - Update high water mark: `wherescape.write_parameter('HWM_{table_name}', new_value)`

5. **Add README.md** with:
   - Required WhereScape parameters
   - Load table naming conventions
   - Host script examples
   - API endpoint documentation

### Incremental Loading Best Practices

- Use WhereScape parameters to track high water marks (dates, IDs, etc.)
- Consider "lookback periods" for data sources that allow late updates (see Friday Pulse connector)
- For date-based incremental: Store `MAX(date_column)` as parameter after each load
- Handle missing/null high water marks (full load fallback)
- The `since_date` or similar parameters in wrapper methods should filter at the API level when possible

### WhereScape Integration

Connectors are executed as "host scripts" in WhereScape RED:

```python
# Host script example (runs in WhereScape context)
from wherescape.connectors.{source}.{source}_load_data import {source}_load_data

{source}_load_data()
```

The WhereScape scheduler:
1. Sets all `WSL_*` environment variables
2. Executes the host script Python file
3. Reads the first line of output (exit code: 1, -1, -2, or -3)
4. Logs subsequent output lines to job log

## Database Patterns

### Metadata Repository Tables

Key WhereScape repository tables:
- `ws_load_tab`: Load table definitions (`lt_obj_key`, `lt_table_name`, `lt_file_path`, `lt_file_name`)
- `ws_load_col`: Load table columns (`lc_obj_key`, `lc_col_name`, `lc_data_type`, `lc_order`)
- `ws_stage_tab`: Stage table definitions
- `ws_stage_col`: Stage table columns
- `ws_fact_tab`, `ws_fact_col`: Fact table definitions (for dimensional modeling)

### Target Database Patterns

All load/stage tables include standard columns:
- `dss_record_source` (varchar): Source identifier
- `dss_load_date` (timestamp): Load timestamp

Data is typically loaded to `load` schema, then transformed to `stage` and `datastore` schemas.

## Common Workflows

### Full Workflow: New API Connector

1. Develop wrapper with API client code
2. Create metadata script and test (creates columns in WhereScape repository)
3. Create load data script with incremental support
4. In WhereScape RED:
   - Create load table
   - Add required parameters (access tokens, high water marks)
   - Create host scripts pointing to your Python functions
   - Attach metadata script to load table and execute
   - Attach load data script to load table
   - Schedule job with proper dependencies

### Debugging Failed Loads

1. Check WhereScape job log for exit code and messages
2. Review rotating log file: `{WSL_WORKDIR}/python_logging/wherescape.log`
3. Run locally using `ws_env.py` to reproduce
4. Common issues:
   - Environment variables not set correctly
   - API authentication failures (check parameters)
   - SQL syntax errors (database-specific SQL)
   - Missing high water mark parameters

## Platform Notes

- **Windows-centric**: Designed to run on Windows WhereScape servers
- **SQL Server repository**: WhereScape repository typically uses SQL Server (note TSQL syntax in stored procedure calls)
- **PostgreSQL target**: Target warehouse typically uses PostgreSQL (note `sslmode=prefer` in connection strings)
- **ODBC connections**: All database connections use pyodbc with DSN-based connections

## Contact

For issues or questions: opensource@wearespindle.com
