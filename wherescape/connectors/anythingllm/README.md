# AnythingLLM Integration

This module provides integration between AnythingLLM and WhereScape RED for loading chat data into the data warehouse.

## Files

- **anythingllm_wrapper.py**: Core API wrapper with functions to fetch chats from AnythingLLM
- **anythingllm_load_data.py**: WhereScape host script to load chat data into the warehouse
- **anythingllm_create_metadata.py**: WhereScape host script to create table metadata
- **anythingllm_test.py**: Standalone test script for testing the API wrapper functions

## Key Functions

### `get_all_embed_chats(embed_uuid, api_key, base_url)`

Fetches all chats from an embed using the embed API endpoint.

**Parameters:**
- `embed_uuid`: The embed UUID
- `api_key`: The API key for authentication
- `base_url`: The base URL for the API endpoint

**Returns:**
- List of flattened dictionaries with 20 data columns + 2 metadata columns

**API Endpoint:** `GET /v1/embed/{embedUuid}/chats`

## Data Structure

The function returns flattened chat data with the following columns:

### Core Fields
- `id` (bigint): Unique chat identifier
- `prompt` (text): User's question/prompt
- `session_id` (text): Session identifier
- `include` (bool): Flag to include/exclude chat
- `embed_id` (int): Embed ID
- `user_id` (int, nullable): User ID
- `created_at` (timestamp): Creation timestamp

### Connection Fields
- `connection_host` (text): Host from connection information
- `connection_ip` (text): IP address from connection information
- `connection_username` (text): Username from connection information

### Response Fields
- `response_text` (text): LLM response text
- `response_type` (text): Response type (e.g., "query")
- `response_attachments` (text): JSON array of attachments
- `response_sources` (text): JSON array of source documents used
- `response_sources_count` (int): Number of sources

### Metrics Fields
- `metrics_completion_tokens` (int): Tokens in completion
- `metrics_prompt_tokens` (int): Tokens in prompt
- `metrics_total_tokens` (int): Total tokens used
- `metrics_output_tps` (numeric): Output tokens per second
- `metrics_duration` (numeric): Response duration in seconds

### Metadata Fields (added by WhereScape)
- `dss_record_source` (varchar): Record source identifier
- `dss_load_date` (timestamp): Load timestamp

## Testing

### Running the Test Script

The `anythingllm_test.py` script allows you to test the API wrapper functions outside of WhereScape.

**Required Environment Variables:**
```bash
export ANYTHINGLLM_EMBED_UUID="your-embed-uuid"
export ANYTHINGLLM_API_KEY="your-api-key"
export ANYTHINGLLM_BASE_URL="https://your-anythingllm-instance.com/api"
```

**Run the test:**
```bash
python anythingllm_test.py
```

**What the test does:**
- Validates that all required environment variables are set
- Calls `get_all_embed_chats()` to fetch all chats from the API
- Displays the total number of chats retrieved
- Shows the first and last chat IDs
- Displays a sample chat record with all fields
- Reports any errors with full stack traces

If environment variables are missing, the script will display a warning message with instructions.

## WhereScape Integration

### Connection Configuration

WhereScape exposes connection details via environment variables prefixed with `WSL_SRCCFG_`:

- `WSL_SRCCFG_USER`: Embed UUID (from the **User** field in the WhereScape connection object) - this is used as the `embed_uuid` parameter in AnythingLLM API calls
- `WSL_SRCCFG_APIKEY`: AnythingLLM API key (from the **API Key** field)
- `WSL_SRCCFG_URL`: AnythingLLM base URL (from the **URL** field, e.g., `https://your-anythingllm-instance.com/api`)

These values are configured in the WhereScape AnythingLLM connection object and automatically exposed as environment variables when the host script runs.

### Creating Metadata

Call `anythingllm_create_metadata()` from a WhereScape host script to automatically create all column definitions in the metadata repository.

### Loading Data

Call `anythingllm_load_data_chats()` from a WhereScape host script to fetch and load chat data.

## API Notes

### Full Load Only

The `/v1/embed/{embedUuid}/chats` endpoint does not support pagination or filtering. It returns all chats for the embed in a single response. Each load will fetch all chats.

### Authentication

Requires Bearer token authentication via the `Authorization` header:
```
Authorization: Bearer YOUR_API_KEY
```

### Endpoint

```
GET https://your-anythingllm-instance.com/api/v1/embed/{embedUuid}/chats
```
