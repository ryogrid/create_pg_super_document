# jsonb_path_query_tz

## Location
src/backend/utils/adt/jsonpath_exec.c: 580 - 590

## Overview
PostgreSQL function that executes a JSONPath query against a JSONB document with timezone-aware operations enabled.

## Definition


## Detailed Description
This function serves as a timezone-aware wrapper for JSONPath query execution. It delegates the actual query processing to  with the timezone flag set to true, enabling timezone-aware datetime operations during path expression evaluation. This allows JSONPath expressions to properly handle datetime operations that depend on timezone information.

## Parameters / Member Variables
- Uses PostgreSQL's function call info structure (PG_FUNCTION_ARGS) which contains:
  - JSONB document to query
  - JSONPath expression 
  - Optional parameters for the path expression

## Dependencies
- Functions called/Symbols referenced:
  - [jsonb_path_query_internal](jsonb_path_query_internal.md) (with timezone=true)
- Called from (representative examples):
  - SQL function calls via PostgreSQL's function manager

## Notes and Other Information
- This is a thin wrapper that enables timezone-aware JSONPath operations
- The actual query logic is implemented in jsonb_path_query_internal
- Located in src/backend/utils/adt/jsonpath_exec.c:580-590
- Timezone awareness affects datetime operations in JSONPath expressions