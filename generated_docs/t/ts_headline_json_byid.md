# ts_headline_json_byid

## Location
src/backend/tsearch/wparser.c: 500 - 508

## Overview
A PostgreSQL wrapper function that generates highlighted headlines from JSON documents using a specified text search configuration, providing a middle-ground interface that accepts explicit configuration but uses default headline options.

## Definition
```c
Datum ts_headline_json_byid(PG_FUNCTION_ARGS)
```

## Detailed Description
This function serves as a wrapper around `ts_headline_json_byid_opt` that allows users to specify a custom text search configuration while using default headline options. It provides more control than `ts_headline_json` by accepting an explicit configuration ID, but is simpler than the full `ts_headline_json_byid_opt` function by not requiring headline options specification. The function takes a text search configuration OID, a JSON document, and a TSQuery, then delegates processing to `ts_headline_json_byid_opt` with a NULL options parameter.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument macro containing:
  - `arg0`: Text search configuration OID to use for headline generation
  - `arg1`: JSON text document to process for headline generation
  - `arg2`: TSQuery specifying the search terms to highlight

## Dependencies
- Functions called/Symbols referenced:
  - [ts_headline_json_byid_opt](ts_headline_json_byid_opt.md): Core function that performs the actual JSON headline generation
  - `DirectFunctionCall3`: PostgreSQL internal function to call another function with 3 arguments
  - `PG_RETURN_DATUM`: PostgreSQL macro for returning a Datum value
  - `PG_GETARG_DATUM`: PostgreSQL macro for extracting function arguments
- Called from (representative examples):
  - No direct references found in the codebase (likely called via SQL function interface)

## Notes and Other Information
- Located in src/backend/tsearch/wparser.c at lines 500-508
- This function provides a balance between simplicity and control for JSON headline generation
- Users can specify a custom text search configuration while still using default highlighting behavior
- No headline options are passed (NULL options parameter), so default highlighting behavior is used
- Part of PostgreSQL full-text search functionality specifically designed for JSON document processing
- The actual headline processing logic is implemented in `ts_headline_json_byid_opt`