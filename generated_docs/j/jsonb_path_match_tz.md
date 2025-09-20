# jsonb_path_match_tz

## Location
[src/backend/utils/adt/jsonpath_exec.c:503-513](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonpath_exec.c#L503-L513)

## Overview
SQL function wrapper that evaluates a JSONPath predicate expression against a JSONB value with timezone-aware datetime operations and returns a boolean result.

## Definition

```c
Datum
jsonb_path_match_tz(PG_FUNCTION_ARGS)
```
## Detailed Description
`jsonb_path_match_tz` is a PostgreSQL SQL function wrapper that provides timezone-aware evaluation of JSONPath predicate expressions against JSONB data. This function is similar to `jsonb_path_match` but enables timezone handling for datetime operations within the JSONPath expression. It delegates the actual work to `jsonb_path_match_internal` with timezone handling enabled (true parameter).

The function follows PostgreSQL's standard function calling convention and is specifically designed for scenarios where JSONPath expressions involve datetime operations that need to be timezone-aware.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument structure containing:

## Dependencies
- Functions called/Symbols referenced:
  - [jsonb_path_match_internal](jsonb_path_match_internal.md)
- Called from (representative examples):
  - SQL function calls through PostgreSQL's function manager

## Notes and Other Information
- This is a timezone-aware version of `jsonb_path_match`
- Timezone handling is enabled (true parameter) for datetime operations
- The actual logic is implemented in `jsonb_path_match_internal`
- Particularly useful for JSONPath expressions involving datetime comparisons
- Located in `src/backend/utils/adt/jsonpath_exec.c:503-513`