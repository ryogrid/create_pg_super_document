# json_string_to_tsvector

## Location
[src/backend/tsearch/to_tsany.c:393-406](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tsearch/to_tsany.c#L393-L406)

## Overview
A PostgreSQL function that converts a JSON string to a text search vector (TSVector) using the current default text search configuration.

## Definition
```c
Datum json_string_to_tsvector(PG_FUNCTION_ARGS)
```

## Detailed Description
This function provides a convenient way to convert JSON string data to a TSVector using the system's default text search configuration. It extracts the JSON text from the PostgreSQL function arguments, retrieves the current default text search configuration using `getTSCurrentConfig`, then delegates the conversion work to `json_to_tsvector_worker` with the `jtiString` flag to indicate that only string values from the JSON should be processed. The function handles proper memory management by freeing the copied text argument and returns the resulting TSVector.

## Parameters / Member Variables
- `json`: The JSON text data to be converted to a TSVector

## Dependencies
- Functions called/Symbols referenced:
  - [getTSCurrentConfig](../g/getTSCurrentConfig.md)
  - [json_to_tsvector_worker](json_to_tsvector_worker.md)
  - PG_GETARG_TEXT_P
  - PG_FREE_IF_COPY
  - PG_RETURN_TSVECTOR
  - jtiString (flag constant)
- Called from (representative examples):
  - No direct callers found in the codebase (likely called via SQL interface)

## Notes and Other Information
- Located in src/backend/tsearch/to_tsany.c:393-406
- This is a PostgreSQL C function that can be called from SQL
- Uses `jtiString` flag to process only string values from JSON, ignoring keys and other data types
- Uses the current default text search configuration rather than taking it as a parameter
- Simpler alternative to `json_string_to_tsvector_byid` when default configuration is sufficient
- Part of PostgreSQL's full-text search functionality for JSON data types
- Properly manages memory by freeing copied text arguments

## Simplified Source

```c
Datum json_string_to_tsvector(PG_FUNCTION_ARGS)
{
    // Extract JSON text from arguments
    text *json = PG_GETARG_TEXT_P(0);

    // Get current text search configuration
    Oid cfgId = getTSCurrentConfig(true);

    // Convert JSON string values to TSVector
    TSVector result = json_to_tsvector_worker(cfgId, json, jtiString);

    // Clean up memory and return result
    PG_FREE_IF_COPY(json, 0);
    PG_RETURN_TSVECTOR(result);
}
```