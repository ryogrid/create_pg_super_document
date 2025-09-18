# ts_headline_byid

## Location
src/backend/tsearch/wparser.c: 339 - 347

## Overview
A PostgreSQL wrapper function that generates highlighted headlines from text based on a text search query, using a specified text search configuration by OID with default formatting options.

## Definition
```c
Datum ts_headline_byid(PG_FUNCTION_ARGS)
```

## Detailed Description
This function serves as a simplified wrapper around ts_headline_byid_opt, providing headline generation functionality without requiring explicit formatting options. It directly delegates to ts_headline_byid_opt using DirectFunctionCall3, passing through the three required arguments (configuration OID, input text, and TSQuery) while omitting the optional formatting parameters.

This design pattern allows PostgreSQL to provide both a simple interface for common use cases and a more complex interface with full customization options, using the same underlying implementation.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument macro that provides:
  - `PG_GETARG_DATUM(0)`: The OID of the text search configuration to use
  - `PG_GETARG_DATUM(1)`: The input text to generate headlines from
  - `PG_GETARG_DATUM(2)`: The text search query for highlighting

## Dependencies
- Functions called/Symbols referenced:
  - [ts_headline_byid_opt](ts_headline_byid_opt.md) (the core headline implementation)
  - PG_RETURN_DATUM (macro to return a Datum result)
  - DirectFunctionCall3 (direct function call with 3 arguments)
  - PG_GETARG_DATUM (macros to extract function arguments)

- Called from (representative examples):
  - This is a top-level PostgreSQL function, typically called from SQL queries

## Notes and Other Information
- This function is part of PostgreSQL's text search functionality
- Acts as a convenience wrapper that uses default formatting options
- More user-friendly than ts_headline_byid_opt for simple use cases
- Uses DirectFunctionCall3 for efficient delegation to the core implementation
- The underlying ts_headline_byid_opt function handles all the actual processing
- Provides a cleaner SQL interface by hiding optional parameter complexity
- Companion to other headline functions like ts_headline and ts_headline_opt