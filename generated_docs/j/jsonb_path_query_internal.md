# jsonb_path_query_internal

## Location
src/backend/utils/adt/jsonpath_exec.c: 526 - 573

## Overview
Internal implementation function that executes a JSONPath expression against a JSONB document and returns matching values as a set-returning function (rowset).

## Definition


## Detailed Description
`jsonb_path_query_internal` is the core implementation function for JSONPath query operations that return multiple results. Unlike the predicate matching functions, this function executes a JSONPath expression and returns all matching values as a rowset using PostgreSQL's set-returning function (SRF) mechanism.

The function operates in two phases:
1. **First call (SRF_IS_FIRSTCALL)**: Executes the JSONPath expression against the JSONB document, collecting all matching values into a list stored in the function context.
2. **Subsequent calls (SRF_PERCALL_SETUP)**: Returns one matching value per call until all results are exhausted.

The function supports timezone-aware datetime operations when the `tz` parameter is true, making it suitable for both timezone-aware and timezone-unaware query operations.

## Parameters / Member Variables
- `fcinfo`: Function call information containing:
  - JSONB document to query (argument 0)
  - JSONPath expression (argument 1) 
  - Variables for path evaluation (argument 2)
  - Silent mode flag (argument 3)
- `tz`: Boolean flag indicating whether to enable timezone-aware datetime operations

## Dependencies
- Functions called/Symbols referenced:
  - executeJsonPath
  - getJsonPathVariableFromJsonb
  - countVariablesFromJsonb
  - JsonValueListGetList
  - JsonbValueToJsonb
  - JsonbPGetDatum
  - SRF_FIRSTCALL_INIT, SRF_PERCALL_SETUP, SRF_RETURN_NEXT, SRF_RETURN_DONE
  - list_head, list_delete_first
  - PG_GETARG_JSONB_P_COPY, PG_GETARG_JSONPATH_P_COPY, PG_GETARG_BOOL
- Called from (representative examples):
  - jsonb_path_query
  - jsonb_path_query_tz

## Notes and Other Information
- Implements PostgreSQL's set-returning function protocol for returning multiple results
- Uses memory context switching to ensure proper memory management across multiple calls
- Supports variable substitution in JSONPath expressions
- Silent mode controls error handling behavior during path evaluation
- Returns each matching JSONB value as a separate row in the result set
- Located in `src/backend/utils/adt/jsonpath_exec.c:526-573`