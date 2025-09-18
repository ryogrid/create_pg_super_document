# json_string_to_tsvector_byid

## Location
src/backend/tsearch/to_tsany.c: 380 - 392

## Overview
A PostgreSQL function that converts a JSON string to a text search vector (TSVector) using a specifically provided text search configuration ID.

## Definition
```c
Datum json_string_to_tsvector_byid(PG_FUNCTION_ARGS)
```

## Detailed Description
This function provides a way to convert JSON string data to a TSVector while explicitly specifying which text search configuration to use via its OID. It extracts the configuration ID and JSON text from the PostgreSQL function arguments, then delegates the conversion work to `json_to_tsvector_worker` with the `jtiString` flag to indicate that only string values from the JSON should be processed. The function handles proper memory management by freeing the copied text argument and returns the resulting TSVector.

## Parameters / Member Variables
- `cfgId`: The OID of the text search configuration to use for processing
- `json`: The JSON text data to be converted to a TSVector

## Dependencies
- Functions called/Symbols referenced:
  - json_to_tsvector_worker
  - PG_GETARG_OID
  - PG_GETARG_TEXT_P
  - PG_FREE_IF_COPY
  - PG_RETURN_TSVECTOR
  - jtiString (flag constant)
- Called from (representative examples):
  - No direct callers found in the codebase (likely called via SQL interface)

## Notes and Other Information
- Located in src/backend/tsearch/to_tsany.c:380-392
- This is a PostgreSQL C function that can be called from SQL
- Uses `jtiString` flag to process only string values from JSON, ignoring keys and other data types
- Allows explicit specification of text search configuration rather than using the default
- Part of PostgreSQL's full-text search functionality for JSON data types
- Properly manages memory by freeing copied text arguments