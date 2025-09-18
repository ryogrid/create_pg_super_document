# jsonb_path_query_first

## Location
src/backend/utils/adt/jsonpath_exec.c: 643 - 648

## Overview
SQL function that executes a JSONPath expression against a JSONB document and returns the first matching result item, or NULL if no items match.

## Definition


## Detailed Description
This function serves as a PostgreSQL SQL-callable wrapper for executing JSONPath queries with the requirement of returning only the first matching result. It delegates the actual implementation to  with timezone handling disabled (tz=false). The function follows PostgreSQL's standard function calling convention using  to access parameters.

The function extracts a JSONB document, JSONPath expression, variable context, and silent mode flag from the function arguments, then executes the path query and returns the first result if any matches are found, otherwise returns NULL.

## Parameters / Member Variables
- Function uses  macro to access:
  - Argument 0: JSONB document to query
  - Argument 1: JSONPath expression to execute
  - Argument 2: JSONB variables context for the path expression
  - Argument 3: Boolean silent flag for error handling

## Dependencies
- Functions called/Symbols referenced:
  - jsonb_path_query_first_internal
- Called from (representative examples):
  - Direct SQL function calls (no internal PostgreSQL callers found)

## Notes and Other Information
- This is a thin wrapper function that simply delegates to the internal implementation
- The function is part of PostgreSQL's JSONPath functionality introduced for JSON processing
- Returns NULL when no matching items are found, making it suitable for optional value extraction
- Located in src/backend/utils/adt/jsonpath_exec.c:643-648
- Timezone handling is explicitly disabled (tz=false) - use jsonb_path_query_first_tz for timezone-aware queries