# jsonb_path_query_array

## Location
[src/backend/utils/adt/jsonpath_exec.c:607-612](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonpath_exec.c#L607-L612)

## Overview
PostgreSQL function that executes a JSONPath query against a JSONB document and returns all matching values as a JSONB array.

## Definition
```c
Datum jsonb_path_query_array(PG_FUNCTION_ARGS)
```

## Detailed Description
This function serves as the public interface for JSONPath array query operations. It delegates the actual query processing to `jsonb_path_query_array_internal` with the timezone flag set to false, meaning it performs timezone-unaware datetime operations. The function returns all values that match the JSONPath expression wrapped in a JSONB array, making it convenient for retrieving multiple matching elements from a JSONB document.

## Parameters / Member Variables
- Uses PostgreSQL's function call info structure (PG_FUNCTION_ARGS) which contains:
  - JSONB document to query
  - JSONPath expression to execute
  - JSONB variables for the path expression
  - Boolean flag for silent mode operation

## Dependencies
- Functions called/Symbols referenced:
  - [jsonb_path_query_array_internal](jsonb_path_query_array_internal.md) (with timezone=false)
- Called from (representative examples):
  - SQL function calls via PostgreSQL's function manager

## Notes and Other Information
- This is a thin wrapper that provides timezone-unaware JSONPath array operations
- The actual query logic is implemented in jsonb_path_query_array_internal
- Always returns results as a JSONB array, even if no matches are found
- Located in src/backend/utils/adt/jsonpath_exec.c:607-612
- Complemented by jsonb_path_query_array_tz for timezone-aware operations