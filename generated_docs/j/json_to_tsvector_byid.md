# json_to_tsvector_byid

## Location
src/backend/tsearch/to_tsany.c: 407 - 422

## Overview
A PostgreSQL function that converts JSON text data to a text search vector (TSVector) using a specifically provided text search configuration ID and custom indexing flags.

## Definition
```c
Datum json_to_tsvector_byid(PG_FUNCTION_ARGS)
```

## Detailed Description
This function provides the most flexible way to convert JSON text data to a TSVector by allowing explicit specification of both the text search configuration and the indexing flags. It extracts three arguments from the PostgreSQL function call interface: the configuration ID, JSON text data, and a JSONB object containing indexing flags. The function parses the indexing flags using `parse_jsonb_index_flags`, then delegates the conversion work to `json_to_tsvector_worker` with the parsed flags. This allows fine-grained control over which parts of the JSON data are processed (keys, values, strings, numbers, etc.). The function handles proper memory management by freeing copied arguments and returns the resulting TSVector.

## Parameters / Member Variables
- `cfgId`: The OID of the text search configuration to use for processing
- `json`: The JSON text data to be converted to a TSVector
- `jbFlags`: A JSONB object containing flags that control how the JSON data is processed during conversion

## Dependencies
- Functions called/Symbols referenced:
  - parse_jsonb_index_flags
  - json_to_tsvector_worker
  - PG_GETARG_OID
  - PG_GETARG_TEXT_P
  - PG_GETARG_JSONB_P
  - PG_FREE_IF_COPY
  - PG_RETURN_TSVECTOR
- Called from (representative examples):
  - No direct callers found in the codebase (likely called via SQL interface)

## Notes and Other Information
- Located in src/backend/tsearch/to_tsany.c:407-422
- This is a PostgreSQL C function that can be called from SQL
- Most flexible variant of JSON to TSVector conversion functions
- Allows explicit specification of both text search configuration and indexing behavior
- Uses parsed flags from JSONB to control which JSON elements are processed
- Part of PostgreSQL's full-text search functionality for JSON data types
- Properly manages memory by freeing both copied text and JSONB arguments