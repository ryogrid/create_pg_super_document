# jsonb_path_query_array_tz

## Location
src/backend/utils/adt/jsonpath_exec.c: 613 - 623

## Overview
PostgreSQL function that executes a JSONPath query against a JSONB document and returns all matching values as a JSONB array with timezone-aware datetime operations.

## Definition
```c
Datum jsonb_path_query_array_tz(PG_FUNCTION_ARGS)
```

## Detailed Description
This function serves as the timezone-aware version of JSONPath array query operations. It delegates the actual query processing to `jsonb_path_query_array_internal` with the timezone flag set to true, enabling proper handling of timezone-aware datetime operations during path expression evaluation. The function returns all values that match the JSONPath expression wrapped in a JSONB array, with datetime computations performed in a timezone-aware manner.

## Parameters / Member Variables
- Uses PostgreSQL's function call info structure (PG_FUNCTION_ARGS) which contains:
  - JSONB document to query
  - JSONPath expression to execute
  - JSONB variables for the path expression  
  - Boolean flag for silent mode operation

## Dependencies
- Functions called/Symbols referenced:
  - [jsonb_path_query_array_internal](jsonb_path_query_array_internal.md) (with timezone=true)
- Called from (representative examples):
  - SQL function calls via PostgreSQL's function manager

## Notes and Other Information
- This is a thin wrapper that enables timezone-aware JSONPath array operations
- The actual query logic is implemented in jsonb_path_query_array_internal
- Always returns results as a JSONB array, even if no matches are found
- Located in src/backend/utils/adt/jsonpath_exec.c:613-623
- Complements jsonb_path_query_array for timezone-unaware operations
- Timezone awareness affects datetime operations and comparisons in JSONPath expressions