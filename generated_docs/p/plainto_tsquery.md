# plainto_tsquery

## Location
src/backend/tsearch/to_tsany.c: 642 - 654

## Overview
A user-facing function that converts plain text to a TSQuery using the current default text search configuration.

## Definition
```c
Datum plainto_tsquery(PG_FUNCTION_ARGS)
```

## Detailed Description
This function serves as a convenient wrapper around plainto_tsquery_byid() that uses the current default text search configuration instead of requiring the user to specify a configuration OID. It automatically retrieves the current text search configuration using getTSCurrentConfig() and then delegates the actual query parsing to plainto_tsquery_byid().

This function is the primary entry point for users who want to convert plain text input into a text search query without having to deal with text search configuration details. It's designed to be simple and intuitive for common use cases where the default configuration is sufficient.

## Parameters / Member Variables
- `PG_GETARG_TEXT_PP(0)`: Input text to be converted into a TSQuery

## Dependencies
- Functions called/Symbols referenced:
  - [getTSCurrentConfig](../g/getTSCurrentConfig.md) (retrieves current text search configuration)
  - [plainto_tsquery_byid](plainto_tsquery_byid.md) (actual implementation function)
  - DirectFunctionCall2 (PostgreSQL function call mechanism)
  - [ObjectIdGetDatum](../O/ObjectIdGetDatum.md) (OID to Datum conversion)
  - [PointerGetDatum](../P/PointerGetDatum.md) (pointer to Datum conversion)
- Called from (representative examples):
  - [ts_match_tt](../t/ts_match_tt.md) (text search matching operations)

## Notes and Other Information
- This is the main user-facing SQL function for plain text to TSQuery conversion
- Uses DirectFunctionCall2 to efficiently call the underlying plainto_tsquery_byid function
- Automatically uses the current session's default text search configuration, making it user-friendly
- Part of PostgreSQL's text search functionality that allows natural language queries without requiring knowledge of TSQuery syntax
- The function is typically accessed through SQL as plainto_tsquery('search text')