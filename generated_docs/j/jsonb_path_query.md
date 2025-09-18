# jsonb_path_query

## Location
[src/backend/utils/adt/jsonpath_exec.c:574-579](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonpath_exec.c#L574-L579)

## Overview
SQL function wrapper that executes a JSONPath expression against a JSONB document and returns all matching values as a rowset.

## Definition


## Detailed Description
`jsonb_path_query` is a PostgreSQL SQL function wrapper that provides the interface for executing JSONPath query expressions against JSONB data and returning multiple matching values. Unlike the predicate matching functions that return boolean results, this function returns all values that match the JSONPath expression as a set of rows.

The function serves as the entry point for the `jsonb_path_query` SQL function and delegates the actual work to `jsonb_path_query_internal` with timezone handling disabled (false parameter). This makes it suitable for JSONPath queries that do not require timezone-aware datetime operations.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument structure containing:
  - JSONB document to query
  - JSONPath expression as text
  - Optional variables for path evaluation
  - Silent mode flag for error handling

## Dependencies
- Functions called/Symbols referenced:
  - [jsonb_path_query_internal](jsonb_path_query_internal.md)
- Called from (representative examples):
  - SQL function calls through PostgreSQL's function manager

## Notes and Other Information
- Returns multiple results as a rowset using PostgreSQL's set-returning function mechanism
- This is a thin wrapper function that provides the SQL interface
- The actual logic is implemented in `jsonb_path_query_internal`
- Timezone handling is disabled (false parameter) unlike timezone-aware variants
- Each matching value is returned as a separate row in the result set
- Located in `src/backend/utils/adt/jsonpath_exec.c:574-579`