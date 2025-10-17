# jsonb_path_query_array_internal

## Location
[src/backend/utils/adt/jsonpath_exec.c:591-606](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonpath_exec.c#L591-L606)

## Overview
Internal implementation function that executes a JSONPath expression against a JSONB document and returns all matching values wrapped in a JSONB array.

## Definition
```c
static Datum jsonb_path_query_array_internal(FunctionCallInfo fcinfo, bool tz)
```

## Detailed Description
This static function serves as the core implementation for JSONPath array query operations. It extracts function arguments, executes the JSONPath expression using `executeJsonPath`, collects all matching values in a JsonValueList, and wraps the results in a JSONB array. The function supports both timezone-aware and timezone-unaware operations based on the `tz` parameter.

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
  - [wrapItemsInArray](../w/wrapItemsInArray.md) (array wrapper)
  - [JsonbValueToJsonb](../J/JsonbValueToJsonb.md) (conversion function)
  - PG_RETURN_JSONB_P (return macro)
- Called from (representative examples):
  - [jsonb_path_query_array](jsonb_path_query_array.md)
  - [jsonb_path_query_array_tz](jsonb_path_query_array_tz.md)

## Notes and Other Information
- This is a static internal function not exposed outside the module
- Results are always returned as a JSONB array, even if no matches are found
- The silent parameter controls error handling during path execution
- Located in src/backend/utils/adt/jsonpath_exec.c:591-606
- Uses JsonValueList to collect intermediate results before array wrapping

## Simplified Source

```c
static Datum jsonb_path_query_array_internal(FunctionCallInfo fcinfo, bool tz) {
    // Extract function arguments
    Jsonb *jb = PG_GETARG_JSONB_P(0);        // JSONB document
    JsonPath *jp = PG_GETARG_JSONPATH_P(1);  // JSONPath expression
    Jsonb *vars = PG_GETARG_JSONB_P(2);      // Variables
    bool silent = PG_GETARG_BOOL(3);         // Silent mode flag

    JsonValueList found = {0};

    // Execute JSONPath and collect all matching values
    executeJsonPath(jp, vars, getJsonPathVariableFromJsonb,
                   countVariablesFromJsonb, jb, !silent, &found, tz);

    // Wrap all found values in a JSONB array and return
    PG_RETURN_JSONB_P(JsonbValueToJsonb(wrapItemsInArray(&found)));
}
```