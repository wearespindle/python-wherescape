# Friday Pulse Integration

This module provides integration between Friday Pulse and WhereScape RED for loading employee happiness survey results into the data warehouse.

## Overview

Friday Pulse is a commercial solution for tracking employee happiness through regular pulse surveys. This integration fetches survey results via the Friday Pulse API and loads them into the WhereScape data warehouse.

More information: https://fridaypulse.com/

## Files

- **friday_pulse_wrapper.py**: Core API wrapper with integrated Friday Pulse client code
- **friday_pulse_load_data.py**: WhereScape host script to load results data into the warehouse
- **friday_pulse_create_metadata.py**: WhereScape host script to create table metadata

## Key Functions

### `get_all_results(bearer_token, since_date=None)`

Fetches all results from Friday Pulse with optional date filtering.

**Parameters:**
- `bearer_token`: The bearer token for API authentication
- `since_date`: Optional date string (YYYY-MM-DD) to filter results created after this date

**Returns:**
- List of flattened dictionaries with 8 data columns + 2 metadata columns

**API Endpoints Used:**
- `GET /api/v1/info/results-dates` - Get available result dates
- `GET /api/v1/results?date={date}` - Get results for a specific date

### `friday_pulse_load_data(lookback_weeks=3)`

Main function to load Friday Pulse data into WhereScape.

**Parameters:**
- `lookback_weeks`: Number of weeks to look back from the high water mark to capture late responses. Default is 3 weeks (21 days).

**Returns:**
- None (data is loaded directly into WhereScape)

## Data Structure

The function returns flattened result data with the following columns:

### Result Fields
- `sample_date` (date): Date when the survey was conducted
- `score` (int): Overall happiness score
- `response_rate` (int): Percentage of responses received
- `response_count` (int): Number of responses received
- `total_count` (int): Total number of potential respondents
- `question_count` (int): Number of questions in the survey

### Topic Fields
- `topic_code` (text): Topic code identifier
- `topic_name` (text): Topic display name

### Metadata Fields (added by WhereScape)
- `dss_record_source` (varchar): Record source identifier
- `dss_load_date` (timestamp): Load timestamp

## Incremental Loading

### Date-based Filtering

The Friday Pulse API supports date filtering natively through the results-dates endpoint. This integration uses the `sample_date` field for incremental loading.

### How It Works

1. **WhereScape Parameter**: A parameter called `HWM_ds_friday_pulse_question` is used to track the latest survey date already loaded.

2. **API Filtering**: The wrapper fetches all available result dates and filters them by comparing against `HWM_ds_friday_pulse_question - lookback_weeks`.

3. **Lookback Period**: Results newer than `HWM_ds_friday_pulse_question - lookback_weeks` are fetched because results can still be updated in Friday Pulse after the survey date. The default lookback period is 3 weeks (21 days), but this can be configured via the `lookback_weeks` parameter.

4. **Efficient Loading**: This approach ensures updated results are captured while minimizing unnecessary data transfer.

5. **Fallback**: If the `HWM_ds_friday_pulse_question` parameter is not set or cannot be read, a full load is performed.

### Setup in WhereScape RED

1. **Create Parameter**: Add a parameter named `HWM_ds_friday_pulse_question` to your WhereScape load table

2. **Initial Load**: Leave `HWM_ds_friday_pulse_question` empty for the first run (full load)

3. **Update Parameter**: After each successful load, update the parameter with:
   ```sql
   SELECT MAX(sample_date) FROM datastore.ds_friday_pulse_question
   ```

4. **Subsequent Loads**: On the next run, only results with `sample_date > HWM_ds_friday_pulse_question - lookback_weeks` will be fetched (default: 3 weeks / 21 days)

### Example Flow

```
First run:
  - HWM_ds_friday_pulse_question parameter: (empty)
  - Action: Full load
  - API calls: Fetch all available dates, get results for each
  - Result: 50 results loaded, max date = 2025-01-16
  - Update parameter: HWM_ds_friday_pulse_question = 2025-01-16

Second run:
  - HWM_ds_friday_pulse_question parameter: 2025-01-16
  - lookback_weeks: 3 (default)
  - Action: Incremental load (fetch dates > 2025-01-16 - 21 days = 2024-12-26)
  - API calls: Fetch results for dates after 2024-12-26 (includes lookback period)
  - Result: 26 results loaded (5 new + 21 days lookback for updates), max date = 2025-01-21
  - Update parameter: HWM_ds_friday_pulse_question = 2025-01-21

Third run:
  - HWM_ds_friday_pulse_question parameter: 2025-01-21
  - lookback_weeks: 3 (default)
  - Action: Incremental load (fetch dates > 2025-01-21 - 21 days = 2024-12-31)
  - API calls: Fetch results for dates after 2024-12-31 (includes lookback period)
  - Result: 21 results loaded (checking for updates in the last 21 days)
  - Parameter unchanged if no newer dates
```
## WhereScape Integration

### Creating Metadata

Call `friday_pulse_create_metadata()` from a WhereScape host script to automatically create all column definitions in the metadata repository.

**Required Environment Variables:**
None (metadata is predefined in the script)

### Loading Data

Call `friday_pulse_load_data(lookback_weeks=3)` from a WhereScape host script to fetch and load results data.

**Parameters:**
- `lookback_weeks`: Optional integer specifying how many weeks to look back from the high water mark (default: 3)

**Required Environment Variables:**
- Friday Pulse API connection

**WhereScape Parameter:**
- `HWM_ds_friday_pulse_question`: Maximum sample date already loaded (optional, for incremental loads)

**Example Usage:**
```python
# Use default 3-week lookback
friday_pulse_load_data()

# Use custom 2-week lookback
friday_pulse_load_data(lookback_weeks=2)

# Use 4-week lookback for more historical updates
friday_pulse_load_data(lookback_weeks=4)
```

## API Notes

### Authentication

Requires Bearer token authentication via the `Authorization` header:
```
Authorization: Bearer YOUR_BEARER_TOKEN
```

### Endpoints

**Get Available Result Dates:**
```
GET https://app.fridaypulse.com/api/v1/info/results-dates
```

**Get Results for a Date:**
```
GET https://app.fridaypulse.com/api/v1/results?date=2025-01-16
```
