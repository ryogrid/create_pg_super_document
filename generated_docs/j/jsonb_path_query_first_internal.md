# jsonb_path_query_first_internal

## Location
[src/backend/utils/adt/jsonpath_exec.c:624-642](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonpath_exec.c#L624-L642)

## Overview
Internal implementation function that executes a JSONPath expression against a JSONB document and returns only the first matching value, or NULL if no matches are found.

## Definition
```c
static Datum jsonb_path_query_first_internal(FunctionCallInfo fcinfo, bool tz)
```

## Detailed Description
This static function serves as the core implementation for JSONPath first-match query operations. It extracts function arguments, executes the JSONPath expression using `executeJsonPath`, and returns only the first matching value from the results. If the JsonValueList contains one or more items, it returns the head (first) item converted to JSONB; otherwise, it returns NULL. The function supports both timezone-aware and timezone-unaware operations based on the `tz` parameter.

## Parameters / Member Variables
- `fcinfo`: Function call information structure containing:
  - Argument 0: JSONB document to query
  - Argument 1: JSONPath expression to execute
  - Argument 2: JSONB variables for the path expression
  - Argument 3: Boolean flag for silent mode (suppresses errors)
- `tz`: Boolean flag indicating whether to use timezone-aware datetime operations

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_JSONB_P (argument extraction)
  - PG_GETARG_JSONPATH_P (path argument extraction)
  - PG_GETARG_BOOL (boolean argument extraction)
  - [executeJsonPath](../e/executeJsonPath.md) (core path execution)
  - [getJsonPathVariableFromJsonb](../g/getJsonPathVariableFromJsonb.md) (variable resolver)
  - [countVariablesFromJsonb](../c/countVariablesFromJsonb.md) (variable counter)
  - [JsonValueListLength](../J/JsonValueListLength.md) (list length check)
  - [JsonValueListHead](../J/JsonValueListHead.md) (first item retrieval)
  - [JsonbValueToJsonb](../J/JsonbValueToJsonb.md) (conversion function)
  - PG_RETURN_JSONB_P (return macro)
  - PG_RETURN_NULL (null return macro)
- Called from (representative examples):
  - [jsonb_path_query_first](jsonb_path_query_first.md)
  - [jsonb_path_query_first_tz](jsonb_path_query_first_tz.md)

## Notes and Other Information
- This is a static internal function not exposed outside the module
- Returns NULL when no matches are found, unlike the array variants
- Only returns the first matching value, ignoring any subsequent matches
- The silent parameter controls error handling during path execution
- Located in src/backend/utils/adt/jsonpath_exec.c:624-642
- Efficiently handles cases where only the first match is needed