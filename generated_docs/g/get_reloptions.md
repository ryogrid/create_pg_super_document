# get_reloptions

## Location
src/backend/utils/adt/ruleutils.c: 13258 - 13312

## Overview
A static utility function that converts a text array datum containing relation options into a formatted C string representation suitable for SQL output.

## Definition
```c
static void
get_reloptions(StringInfo buf, Datum reloptions)
```

## Detailed Description
This function processes a PostgreSQL text array datum containing relation options (such as storage parameters for tables, indexes, etc.) and formats them into a human-readable string. Each element in the input array should have the format "name=value". The function parses these options and constructs a properly quoted, comma-separated string suitable for inclusion in SQL statements. It handles proper quoting of both option names and values, using `quote_identifier()` for names and applying appropriate quoting rules for values to avoid unnecessary clutter while ensuring correctness.

## Parameters / Member Variables
- `buf`: A StringInfo buffer where the formatted options string will be appended
- `reloptions`: A Datum containing a text array of relation options in "name=value" format

## Dependencies
- Functions called/Symbols referenced:
  - deconstruct_array_builtin
  - DatumGetArrayTypeP
  - TextDatumGetCString
  - strchr
  - quote_identifier
  - simple_quote_literal
  - appendStringInfoString
  - appendStringInfo
  - pfree
- Called from (representative examples):
  - pg_get_indexdef_worker
  - flatten_reloptions

## Notes and Other Information
- This is a static function within ruleutils.c, primarily used for SQL object definition reconstruction
- Handles malformed options gracefully by treating missing "=" as empty values
- Applies intelligent quoting: option names are always quoted with `quote_identifier()`, but values are only quoted if necessary
- The function avoids unnecessary quoting for simple identifier-like values to reduce clutter in generated SQL
- Memory management: properly frees temporary strings allocated during processing
- Used in contexts where PostgreSQL needs to display or reconstruct storage options for relations