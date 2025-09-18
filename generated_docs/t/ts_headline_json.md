# ts_headline_json

## Location
src/backend/tsearch/wparser.c: 491 - 499

## Overview
A PostgreSQL wrapper function that generates highlighted headlines from JSON documents using the current default text search configuration, providing a simplified interface without requiring explicit configuration or options specification.

## Definition
```c
Datum ts_headline_json(PG_FUNCTION_ARGS)
```

## Detailed Description
This function serves as a convenience wrapper around `ts_headline_json_byid_opt` that automatically uses the current default text search configuration and no additional options. It simplifies JSON headline generation by eliminating the need for users to specify a text search configuration explicitly. The function takes only a JSON document and a TSQuery, then delegates processing to `ts_headline_json_byid_opt` with the default configuration ID obtained from `getTSCurrentConfig(true)` and a NULL options parameter.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument macro containing:
  - `arg0`: JSON text document to process for headline generation
  - `arg1`: TSQuery specifying the search terms to highlight

## Dependencies
- Functions called/Symbols referenced:
  - [ts_headline_json_byid_opt](ts_headline_json_byid_opt.md): Core function that performs the actual JSON headline generation
  - `DirectFunctionCall3`: PostgreSQL internal function to call another function with 3 arguments
  - [getTSCurrentConfig](../g/getTSCurrentConfig.md): Retrieves the current default text search configuration OID
  - `PG_RETURN_DATUM`: PostgreSQL macro for returning a Datum value
  - [ObjectIdGetDatum](../O/ObjectIdGetDatum.md): Converts OID to Datum format
  - `PG_GETARG_DATUM`: PostgreSQL macro for extracting function arguments
- Called from (representative examples):
  - No direct references found in the codebase (likely called via SQL function interface)

## Notes and Other Information
- Located in src/backend/tsearch/wparser.c at lines 491-499
- This is the most basic wrapper function for JSON headline generation, requiring only essential parameters
- The function automatically determines the appropriate text search configuration using `getTSCurrentConfig(true)`
- No headline options are passed (NULL options parameter), so default highlighting behavior is used
- Part of PostgreSQL full-text search functionality specifically designed for JSON document processing
- The actual headline processing logic is implemented in `ts_headline_json_byid_opt`