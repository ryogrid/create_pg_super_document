# json_extract_path_text

## Location
src/backend/utils/adt/jsonfuncs.c: 1013 - 1021

## Overview
Extracts a JSON value from a JSON document following a specified path of object keys and array indices, returning the result as text with JSON string values unescaped.

## Definition
```c
Datum json_extract_path_text(PG_FUNCTION_ARGS)
```

## Detailed Description
This function extracts a value from a JSON document by following a specified path consisting of object field names and array indices, and converts the result to text. It's a simple wrapper around the `get_path_all` function with the `as_text` parameter set to true, meaning it returns the extracted value as text rather than JSON.

For JSON string values, the function removes JSON escaping and quotes. For other JSON values (numbers, booleans, null), it converts them to their text representation. This provides a convenient way to extract values from complex JSON structures and use them as plain text.

## Parameters / Member Variables
- `fcinfo`: PostgreSQL function call information structure containing:
  - First argument: JSON document (text)
  - Remaining arguments: Path components as text values (object keys or array indices)

## Dependencies
- Functions called/Symbols referenced:
  - `[get_path_all](../g/get_path_all.md)` - Common worker function for JSON path extraction operations
- Called from (representative examples):
  - No direct references found (used via SQL function calls)

## Notes and Other Information
- This is a thin wrapper around `get_path_all` with `as_text=true`
- Returns the extracted value as text, with JSON strings unescaped and other values converted to text
- Supports navigation through both JSON objects (using string keys) and arrays (using integer indices)
- Returns NULL if the path doesn't exist or if any intermediate step in the path fails
- Part of PostgreSQL's JSON path extraction functionality
- The function is registered as a PostgreSQL built-in function and accessible via SQL
- Complements `json_extract_path` which returns the result as JSON instead of text
- Particularly useful when you need to extract string values from JSON for use in text operations or when you want a plain text representation of JSON values