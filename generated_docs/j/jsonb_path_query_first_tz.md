# jsonb_path_query_first_tz

## Location
src/backend/utils/adt/jsonpath_exec.c: 649 - 678

## Overview
SQL function that executes a JSONPath expression against a JSONB document with timezone-aware processing and returns the first matching result item, or NULL if no items match.

## Definition
```c
Datum jsonb_path_query_first_tz(PG_FUNCTION_ARGS)
```

## Detailed Description
This function is the timezone-aware variant of `jsonb_path_query_first`. It serves as a PostgreSQL SQL-callable wrapper for executing JSONPath queries with timezone support enabled. Like its non-timezone counterpart, it delegates to `jsonb_path_query_first_internal` but passes `true` for the timezone parameter, enabling proper handling of datetime operations in JSONPath expressions that depend on timezone context.

The function extracts the same parameters as the non-timezone version but processes datetime-related JSONPath operations with timezone awareness, making it suitable for applications that need to handle time-sensitive JSON data correctly across different timezones.

## Parameters / Member Variables
- Function uses `PG_FUNCTION_ARGS` macro to access:
  - Argument 0: JSONB document to query
  - Argument 1: JSONPath expression to execute  
  - Argument 2: JSONB variables context for the path expression
  - Argument 3: Boolean silent flag for error handling

## Dependencies
- Functions called/Symbols referenced:
  - [jsonb_path_query_first_internal](jsonb_path_query_first_internal.md)
  - [JsonPathExecResult](../J/JsonPathExecResult.md) (referenced in extended function body)
- Called from (representative examples):
  - Direct SQL function calls (no internal PostgreSQL callers found)

## Notes and Other Information
- This is a timezone-aware wrapper that enables proper datetime handling in JSONPath expressions
- The key difference from `jsonb_path_query_first` is the `tz=true` parameter passed to the internal implementation
- Essential for applications dealing with JSON data containing timestamps or date operations that need timezone context
- Returns NULL when no matching items are found, maintaining consistency with the non-timezone variant
- Located in src/backend/utils/adt/jsonpath_exec.c:649-678
- Part of PostgreSQL's comprehensive JSONPath timezone support infrastructure