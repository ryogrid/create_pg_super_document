# ts_headline_json_opt

## Location
src/backend/tsearch/wparser.c: 509 - 522

## Overview
A PostgreSQL wrapper function that generates highlighted headlines from JSON documents using the current default text search configuration with customizable headline generation options, providing full functionality while using system defaults for configuration.

## Definition
```c
Datum ts_headline_json_opt(PG_FUNCTION_ARGS)
```

## Detailed Description
This function serves as a wrapper around `ts_headline_json_byid_opt` that automatically uses the current default text search configuration while allowing users to specify custom headline generation options. It provides the most convenient interface for JSON headline generation with full customization capabilities, eliminating the need to specify a text search configuration explicitly while still supporting advanced headline options. The function takes a JSON document, a TSQuery, and headline options, then delegates processing to `ts_headline_json_byid_opt` with the default configuration ID obtained from `getTSCurrentConfig(true)`.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument macro containing:
  - `arg0`: JSON text document to process for headline generation
  - `arg1`: TSQuery specifying the search terms to highlight
  - `arg2`: Text string containing headline generation options

## Dependencies
- Functions called/Symbols referenced:
  - `[ts_headline_json_byid_opt](ts_headline_json_byid_opt.md)`: Core function that performs the actual JSON headline generation
  - `DirectFunctionCall4`: PostgreSQL internal function to call another function with 4 arguments
  - `[getTSCurrentConfig](../g/getTSCurrentConfig.md)`: Retrieves the current default text search configuration OID
  - `PG_RETURN_DATUM`: PostgreSQL macro for returning a Datum value
  - `[ObjectIdGetDatum](../O/ObjectIdGetDatum.md)`: Converts OID to Datum format
  - `PG_GETARG_DATUM`: PostgreSQL macro for extracting function arguments
- Called from (representative examples):
  - No direct references found in the codebase (likely called via SQL function interface)

## Notes and Other Information
- Located in src/backend/tsearch/wparser.c at lines 509-522
- This function combines the convenience of automatic configuration selection with the flexibility of custom headline options
- The function automatically determines the appropriate text search configuration using `getTSCurrentConfig(true)`
- Users can specify custom headline options while still benefiting from automatic configuration selection
- Part of PostgreSQL full-text search functionality specifically designed for JSON document processing
- The actual headline processing logic is implemented in `ts_headline_json_byid_opt`
- This provides the most user-friendly interface for JSON headline generation with customization options