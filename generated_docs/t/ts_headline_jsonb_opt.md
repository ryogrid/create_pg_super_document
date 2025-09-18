# ts_headline_jsonb_opt

## Location
[src/backend/tsearch/wparser.c:433-442](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tsearch/wparser.c#L433-L442)

## Overview
A PostgreSQL function that generates highlighted headlines from JSONB documents using the current default text search configuration, providing an interface that automatically applies the system default configuration without requiring explicit configuration specification.

## Definition
```c
Datum ts_headline_jsonb_opt(PG_FUNCTION_ARGS)
```

## Detailed Description
This function serves as a wrapper around `ts_headline_jsonb_byid_opt` that automatically uses the current default text search configuration. It simplifies the headline generation process by eliminating the need for users to specify a text search configuration explicitly. The function takes a JSONB document, a tsquery, and headline options, then delegates the actual processing to `ts_headline_jsonb_byid_opt` with the default configuration ID obtained from `getTSCurrentConfig(true)`.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument macro containing:
  - `arg0`: JSONB document to process for headline generation
  - `arg1`: TSQuery specifying the search terms to highlight
  - `arg2`: Text string containing headline generation options

## Dependencies
- Functions called/Symbols referenced:
  - [ts_headline_jsonb_byid_opt](ts_headline_jsonb_byid_opt.md): Core function that performs the actual headline generation
  - `DirectFunctionCall4`: PostgreSQL internal function to call another function with 4 arguments
  - [getTSCurrentConfig](../g/getTSCurrentConfig.md): Retrieves the current default text search configuration OID
  - `PG_RETURN_DATUM`: PostgreSQL macro for returning a Datum value
  - [ObjectIdGetDatum](../O/ObjectIdGetDatum.md): Converts OID to Datum format
  - `PG_GETARG_DATUM`: PostgreSQL macro for extracting function arguments
- Called from (representative examples):
  - No direct references found in the codebase (likely called via SQL function interface)

## Notes and Other Information
- Located in src/backend/tsearch/wparser.c at lines 433-442
- This is a convenience wrapper function that reduces the complexity of calling headline generation functions
- The function automatically determines the appropriate text search configuration using `getTSCurrentConfig(true)`
- Part of PostgreSQL text search functionality for generating highlighted snippets from JSONB documents
- The actual headline processing logic is implemented in `ts_headline_jsonb_byid_opt`