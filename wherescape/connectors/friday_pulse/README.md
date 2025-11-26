# Friday Pulse Integration

This module provides integration between Friday Pulse and WhereScape RED for loading employee happiness survey data into the data warehouse.

## Overview

Friday Pulse is a commercial solution for tracking employee happiness through regular pulse surveys. This integration fetches survey data via the Friday Pulse API and loads them into the WhereScape data warehouse.

More information: https://fridaypulse.com/

## Files

- **friday_pulse_wrapper.py**: Core API wrapper with integrated Friday Pulse client code
- **friday_pulse_load_data.py**: WhereScape host script to load data into the warehouse
- **friday_pulse_create_metadata.py**: WhereScape host script to create table metadata

## Available Data Endpoints

The Friday Pulse API provides access to several data types:

### Reference Data
- **Topics** (`get_topics()`) - Survey topic definitions
- **Group Types** (`get_group_types()`) - Types of organizational groups
- **Groups** (`get_groups()`) - All groups across all group types

### Survey Results
- **General Results** (`get_general_results()`) - Company-wide survey results by date and topic
- **Group Results** (`get_group_results()`) - Survey results broken down by group
- **General Notes** (`get_general_notes()`) - Qualitative notes not associated with specific groups
- **Group Notes** (`get_group_notes()`) - Qualitative notes associated with group results
- **General Risks** (`get_general_risks()`) - General risk data from the API
- **Group Risks** (`get_group_risks()`) - Risk data broken down by group

All methods support optional `since_date` parameter for incremental loading.

## API Client Usage

### `FridayPulseClient(bearer_token)`

Initialize the client with a bearer token:

```python
from wherescape.connectors.friday_pulse.friday_pulse_wrapper import FridayPulseClient

client = FridayPulseClient(bearer_token="YOUR_TOKEN")
```

### Simple Data Retrieval Methods

**Get Topics:**
```python
topics = client.get_topics()
# Returns: List of flattened topic dictionaries (code, name)
```

**Get Group Types:**
```python
group_types = client.get_group_types()
# Returns: List of flattened group type dictionaries (code, name)
```

**Get Groups:**
```python
groups = client.get_groups()
# Returns: List of flattened group dictionaries (id, name, group_type_code, etc.)
```

**Get Available Result Dates:**
```python
dates = client.get_results_dates()
# Returns: List of dictionaries with 'date' and 'question_count' keys
```

### Complex Data Retrieval Methods (with Date Filtering)

**Get General Results:**
```python
# Full load
results = client.get_general_results()

# Incremental load (only dates after 2025-01-01)
results = client.get_general_results(since_date="2025-01-01")
```

**Get Group Results:**
```python
# Full load
results = client.get_group_results()

# Incremental load (only dates after 2025-01-01)
results = client.get_group_results(since_date="2025-01-01")
```

**Get General Notes:**
```python
# Full load
notes = client.get_general_notes()

# Incremental load (only dates after 2025-01-01)
notes = client.get_general_notes(since_date="2025-01-01")
```

**Get Group Notes:**
```python
# Full load
notes = client.get_group_notes()

# Incremental load (only dates after 2025-01-01)
notes = client.get_group_notes(since_date="2025-01-01")
```

**Get General Risks:**
```python
# Full load
risks = client.get_general_risks()

# Incremental load (only dates after 2025-01-01)
risks = client.get_general_risks(since_date="2025-01-01")
```

**Get Group Risks:**
```python
# Full load
risks = client.get_group_risks()

# Incremental load (only dates after 2025-01-01)
risks = client.get_group_risks(since_date="2025-01-01")
```

## WhereScape Integration

### `friday_pulse_load_data(lookback_weeks=3)`

Main function to load Friday Pulse data into WhereScape. Automatically detects which data type to load based on the table name.

**Parameters:**
- `lookback_weeks`: Number of weeks to look back from the high water mark to capture late responses. Default is 3 weeks (21 days).

**Returns:**
- None (data is loaded directly into WhereScape)

## Data Structures

All data is returned as flattened dictionaries ready for database insertion. WhereScape automatically adds metadata columns.

### Topics
- `code` (text): Topic code identifier
- `name` (text): Topic display name

### Group Types
- `code` (text): Group type code identifier
- `name` (text): Group type display name

### Groups
- `id` (bigint): Unique group identifier
- `name` (text): Group name
- `group_type_code` (text): Associated group type code
- Additional fields may be present depending on API response

### General Results
- `sample_date` (date): Survey date
- `score` (numeric): Overall happiness score
- `response_rate` (numeric): Percentage of responses received
- `response_count` (int): Number of responses received
- `total_count` (int): Total number of potential respondents
- `question_count` (int): Number of questions in the survey
- `topic_code` (text): Topic code identifier
- `topic_name` (text): Topic display name

### Group Results
Same structure as General Results, plus:
- `group_id` (bigint): Associated group identifier

### General Notes
- Additional note fields depending on API response
- Typically includes fields like `id`, `action`, `text`, `flow_date`, `created_at`, `author_name`, `author_email`, `recipients_name`, `recipients_email`

### Group Notes
- `group_id` (bigint): Associated group identifier
- Additional note fields depending on API response

### General Risks
- Additional risk fields depending on API response

### Group Risks
- `group_id` (bigint): Associated group identifier
- Additional risk fields depending on API response

### Metadata Fields (added by WhereScape)
- `dss_record_source` (varchar): Record source identifier
- `dss_load_date` (timestamp): Load timestamp

## Supported Load Tables

The integration automatically detects which data to load based on the table name. Supported patterns:

### Reference Data (No Incremental Loading)
- **topics**: Table name contains "topics" → loads topic definitions
- **group_types**: Table name contains "group_types" → loads group type definitions
- **groups**: Table name contains "groups" (but not "group_results" or "group_notes") → loads all groups

### Survey Data (Supports Incremental Loading)
- **general_results** or **results**: Table name contains "general_results" or "results" → loads company-wide results
- **group_results**: Table name contains "group_results" → loads results by group
- **general_notes**: Table name contains "general_notes" → loads general notes
- **group_notes**: Table name contains "group_notes" → loads notes by group
- **general_risks**: Table name contains "general_risks" → loads general risk data
- **group_risks**: Table name contains "group_risks" → loads risk data by group

### Table Naming Examples
```
load_friday_pulse_topics          → loads topics
load_friday_pulse_group_types     → loads group types
load_friday_pulse_groups          → loads groups
load_friday_pulse_general_results → loads general results (incremental)
load_friday_pulse_group_results   → loads group results (incremental)
load_friday_pulse_general_notes   → loads general notes (incremental)
load_friday_pulse_group_notes     → loads group notes (incremental)
load_friday_pulse_general_risks   → loads general risks (incremental)
load_friday_pulse_group_risks     → loads group risks (incremental)
```

## Incremental Loading

### Date-based Filtering

Survey data tables (general_results, group_results, group_notes) support incremental loading using the `sample_date` field.

### How It Works

1. **WhereScape Parameter**: A parameter named `HWM_{table}` tracks the latest survey date loaded.
   - If table name starts with `load_`, the prefix is changed to `ds_` for the parameter name
   - Example: `load_friday_pulse_general_results` → parameter `HWM_ds_friday_pulse_general_results`

2. **API Filtering**: The wrapper fetches all available result dates and filters them by comparing against `HWM_{table} - lookback_weeks`.

3. **Lookback Period**: Results newer than `HWM_{table} - lookback_weeks` are fetched because survey results can be updated after the survey date (late responses). The default lookback period is 3 weeks (21 days), configurable via the `lookback_weeks` parameter.

4. **Efficient Loading**: This approach ensures updated results are captured while minimizing unnecessary data transfer.

5. **Fallback**: If the HWM parameter is not set or cannot be read, a full load is performed.

### Setup in WhereScape RED

1. **Create Parameter**: Add a parameter for your load table following the naming pattern:
   - For `load_friday_pulse_general_results`: Create `HWM_ds_friday_pulse_general_results`
   - For `load_friday_pulse_group_results`: Create `HWM_ds_friday_pulse_group_results`
   - For `load_friday_pulse_group_notes`: Create `HWM_ds_friday_pulse_group_notes`

2. **Initial Load**: Leave the HWM parameter empty for the first run (full load)

3. **Update Parameter**: After each successful load, update the parameter with:
   ```sql
   SELECT MAX(sample_date) FROM datastore.{target_table}
   ```

4. **Subsequent Loads**: Only results with `sample_date > HWM - lookback_weeks` will be fetched

### Example Flow (General Results)

```
First run:
  - HWM_ds_friday_pulse_general_results parameter: (empty)
  - Action: Full load
  - API calls: Fetch all available dates, get results for each
  - Result: 150 results loaded, max date = 2025-01-16
  - Update parameter: HWM_ds_friday_pulse_general_results = 2025-01-16

Second run:
  - HWM_ds_friday_pulse_general_results parameter: 2025-01-16
  - lookback_weeks: 3 (default)
  - Action: Incremental load (fetch dates > 2025-01-16 - 21 days = 2024-12-26)
  - API calls: Fetch results for dates after 2024-12-26 (includes lookback period)
  - Result: 45 results loaded (15 new + 30 from lookback for updates), max date = 2025-01-21
  - Update parameter: HWM_ds_friday_pulse_general_results = 2025-01-21

Third run:
  - HWM_ds_friday_pulse_general_results parameter: 2025-01-21
  - lookback_weeks: 3 (default)
  - Action: Incremental load (fetch dates > 2025-01-21 - 21 days = 2024-12-31)
  - API calls: Fetch results for dates after 2024-12-31
  - Result: 30 results loaded (checking for updates in the last 21 days)
  - Update parameter: HWM_ds_friday_pulse_general_results = 2025-01-21 (unchanged if no newer dates)
```
## WhereScape Host Script Setup

### Creating Metadata

Create a WhereScape host script that calls `friday_pulse_create_metadata()`:

```python
from wherescape.connectors.friday_pulse.friday_pulse_create_metadata import friday_pulse_create_metadata

friday_pulse_create_metadata()
```

This automatically creates all column definitions in the WhereScape metadata repository based on actual API response data.

**Required Environment Variables:**
- `WSL_SRCCFG_APIKEY`: Friday Pulse bearer token
- Standard WhereScape environment variables (set automatically by WhereScape)

### Loading Data

Create a WhereScape host script that calls `friday_pulse_load_data()`:

```python
from wherescape.connectors.friday_pulse.friday_pulse_load_data import friday_pulse_load_data

# Use default 3-week lookback
friday_pulse_load_data()

# Or with custom lookback period
friday_pulse_load_data(lookback_weeks=2)
```

**Parameters:**
- `lookback_weeks`: Optional integer specifying how many weeks to look back from the high water mark (default: 3)

**Required Environment Variables:**
- `WSL_SRCCFG_APIKEY`: Friday Pulse bearer token
- Standard WhereScape environment variables (set automatically by WhereScape)

**WhereScape Parameters (for incremental loads):**
- Reference data tables: No parameters needed (always full load)
- Survey data tables: Create `HWM_{table_name}` parameter (e.g., `HWM_ds_friday_pulse_general_results`)

## API Reference

### Authentication

All API requests require Bearer token authentication via the `Authorization` header:
```
Authorization: Bearer YOUR_BEARER_TOKEN
```

### Base URL
```
https://app.fridaypulse.com/
```

### Endpoints

#### Reference Data

**Get Topics:**
```
GET /api/v1/topics
```
Returns list of available survey topics.

**Get Group Types:**
```
GET /api/v1/group-types
```
Returns list of group type definitions.

**Get Groups for a Group Type:**
```
GET /api/v1/group-types/{group_type_code}/groups
```
Returns list of groups for a specific group type.

#### Survey Data

**Get Available Result Dates:**
```
GET /api/v1/info/results-dates
```
Returns list of available survey dates with question counts.

**Get General Results:**
```
GET /api/v1/results?date={date}
```
Returns company-wide survey results for a specific date (YYYY-MM-DD).

**Get Group Results:**
```
GET /api/v1/groups/{group_id}/results?date={date}
```
Returns survey results for a specific group and date.

**Get General Notes:**
```
GET /api/v1/notes
GET /api/v1/notes?date={date}
```
Returns general notes (not associated with groups). Optional date parameter filters to specific date.

**Get Group Notes:**
```
GET /api/v1/groups/{group_id}/notes
GET /api/v1/groups/{group_id}/notes?date={date}
```
Returns qualitative notes for a specific group. Optional date parameter filters to specific date.

**Get General Risks:**
```
GET /api/v1/risk
GET /api/v1/risk?date={date}
```
Returns general risk data. Optional date parameter filters to specific date.

**Get Group Risks:**
```
GET /api/v1/groups/{group_id}/risk
GET /api/v1/groups/{group_id}/risk?date={date}
```
Returns risk data for a specific group. Optional date parameter filters to specific date.

### Response Format

All endpoints return JSON. The Friday Pulse wrapper automatically flattens nested JSON structures for easy database insertion.

### Error Handling

The integration handles common API errors:
- **Timeout**: Logs warning and continues with next item
- **500 Internal Server Error**: Logs error and continues with next item (common when no data exists for a date/group)
- **RequestException**: Logs error and continues with next item

### Rate Limiting

The API does not appear to have explicit rate limiting, but the integration processes requests sequentially to avoid overwhelming the service.
