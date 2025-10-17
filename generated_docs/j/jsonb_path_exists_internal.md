# jsonb_path_exists_internal

## Location
[src/backend/utils/adt/jsonpath_exec.c:399-426](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonpath_exec.c#L399-L426)

## Overview
Internal implementation function that checks whether a JSONPath expression returns at least one item for the specified JSONB value, supporting both timezone-aware and timezone-naive operations.

## Definition

```c
static Datum
jsonb_path_exists_internal(FunctionCallInfo fcinfo, bool tz)
```
## Detailed Description
This is a core internal function that implements the JSONPath existence check functionality for PostgreSQL's JSONB data type. The function is designed to support both the @? and @@ operators and is optimized for consistency between index scan and sequential scan results. It follows a "throw as few errors as possible" philosophy to make indexing more reliable.

The function executes a JSONPath expression against a JSONB value and returns true if the path returns at least one matching item. The behavior aligns with the SQL/JSON JSON_EXISTS() clause specification. The function handles optional variables and silent mode parameters, and supports both timezone-aware and timezone-naive operations based on the  parameter.

## Parameters / Member Variables
- : Function call information containing the arguments passed to the function
- : Boolean flag indicating whether to use timezone-aware JSONPath execution

Function arguments accessed through :
- Argument 0:  - The JSONB value to search within  
- Argument 1:  - The JSONPath expression to execute
- Argument 2 (optional):  - Variables for JSONPath execution
- Argument 3 (optional):  - Whether to suppress errors during execution

## Dependencies
- Functions called/Symbols referenced:
  -  - Extract JSONB argument from function call info
  -  - Extract JSONPath argument from function call info
  -  - Extract boolean argument from function call info
  -  - Get number of arguments passed to function
  -  - Core JSONPath execution engine
  -  - [Variable](../V/Variable.md) resolver for JSONPath execution
  -  - Count variables in JSONB context
  -  - Check if JSONPath execution result is an error
  -  - Memory management for varlena types
  -  - Return NULL value
  -  - Return boolean value

- Called from:
  -  (src/backend/utils/adt/jsonpath_exec.c:429)
  -  (src/backend/utils/adt/jsonpath_exec.c:435)
  -  (src/backend/utils/adt/jsonpath_exec.c:447)

## Notes and Other Information
- This function is marked as , meaning it's only accessible within the same compilation unit
- The function is designed to minimize error throwing to improve index scan consistency
- Returns NULL when JSONPath execution results in an error (jperIsError)
- Returns true only when the execution result is  (successful with at least one match)
- Memory management is handled through  for the input JSONB and JSONPath arguments
- The  parameter defaults to , promoting error suppression for better indexing behavior
- The function supports both 2-argument and 4-argument calling conventions

## Simplified Source

```c
static Datum
jsonb_path_exists_internal(FunctionCallInfo fcinfo, bool tz)
{
    Jsonb *jb = PG_GETARG_JSONB_P(0);
    JsonPath *jp = PG_GETARG_JSONPATH_P(1);
    JsonPathExecResult res;
    Jsonb *vars = NULL;
    bool silent = true;

    // Check for optional variables and silent mode arguments
    if (PG_NARGS() == 4) {
        vars = PG_GETARG_JSONB_P(2);
        silent = PG_GETARG_BOOL(3);
    }

    // Execute the JSONPath expression
    res = executeJsonPath(jp, vars, getJsonPathVariableFromJsonb,
                         countVariablesFromJsonb,
                         jb, !silent, NULL, tz);

    // Clean up memory
    PG_FREE_IF_COPY(jb, 0);
    PG_FREE_IF_COPY(jp, 1);

    // Return NULL on error, boolean result on success
    if (jperIsError(res))
        PG_RETURN_NULL();

    PG_RETURN_BOOL(res == jperOk);
}
```