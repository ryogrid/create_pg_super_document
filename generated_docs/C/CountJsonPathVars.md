# CountJsonPathVars

## Location
[src/backend/utils/adt/jsonpath_exec.c:3036-3048](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonpath_exec.c#L3036-L3048)

## Overview
Returns the count of JSON path variables available in the execution context.

## Definition

```c
static int
CountJsonPathVars(void *cxt)
```
## Detailed Description
This is a simple utility function that counts the number of variables available for jsonpath execution. It takes a void pointer that represents the variable context (which is actually a List of JsonPathVariable structures) and returns the length of that list using PostgreSQL's list_length function. This count is typically used by the jsonpath execution engine to determine how many variables are available for variable resolution operations.

## Parameters / Member Variables
- : void pointer that is cast to a List containing JsonPathVariable structures

## Dependencies
- Functions called/Symbols referenced:
  - list_length (PostgreSQL list utility function to get list length)
- Data types used:
  - [List](../L/List.md) (PostgreSQL list structure)
- Called from (representative examples):
  - RETURN_ERROR macro in jsonpath_exec.c:314
  - [JsonPathExists](../J/JsonPathExists.md) in jsonpath_exec.c:3893
  - [JsonPathQuery](../J/JsonPathQuery.md) in jsonpath_exec.c:3922
  - [JsonPathValue](../J/JsonPathValue.md) in jsonpath_exec.c:4013
  - [JsonTableResetRowPattern](../J/JsonTableResetRowPattern.md) in jsonpath_exec.c:4267

## Notes and Other Information
- This is a static function internal to the jsonpath execution module
- Very simple function that serves as a wrapper around list_length for type safety
- Used by higher-level jsonpath functions that need to know the variable count
- Part of PostgreSQL's SQL/JSON path expression variable management system
- The function assumes the context parameter is always a valid List pointer