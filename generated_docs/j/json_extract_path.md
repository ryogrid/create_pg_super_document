# json_extract_path

## Location
src/backend/utils/adt/jsonfuncs.c: 1007 - 1012

## Overview
Extracts a JSON value from a JSON document following a specified path of object keys and array indices, returning the result as JSON.

## Definition
```c
Datum json_extract_path(PG_FUNCTION_ARGS)
```

## Detailed Description
This function extracts a value from a JSON document by following a specified path consisting of object field names and array indices. It's a simple wrapper around the `get_path_all` function with the `as_text` parameter set to false, meaning it returns the extracted value as JSON rather than converting it to text.

The function accepts a JSON document and a variable number of path components (text values representing object keys or array indices) and navigates through the JSON structure to extract the value at the specified location.

## Parameters / Member Variables
- `fcinfo`: PostgreSQL function call information structure containing:
  - First argument: JSON document (text)
  - Remaining arguments: Path components as text values (object keys or array indices)

## Dependencies
- Functions called/Symbols referenced:
  - `get_path_all` - Common worker function for JSON path extraction operations
- Called from (representative examples):
  - No direct references found (used via SQL function calls)

## Notes and Other Information
- This is a thin wrapper around `get_path_all` with `as_text=false`
- Returns the extracted value as JSON, preserving the original JSON formatting
- Supports navigation through both JSON objects (using string keys) and arrays (using integer indices)
- Returns NULL if the path doesn't exist or if any intermediate step in the path fails
- Part of PostgreSQL's JSON path extraction functionality
- The function is registered as a PostgreSQL built-in function and accessible via SQL
- Complements `json_extract_path_text` which returns the result as text instead of JSON