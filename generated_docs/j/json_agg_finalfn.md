# json_agg_finalfn

## Location
[src/backend/utils/adt/json.c:870-890](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/json.c#L870-L890)

## Overview
The  function serves as the final function for the  aggregate, responsible for completing the JSON array construction by adding the closing bracket and returning the final result.

## Definition

```c
Datum
json_agg_finalfn(PG_FUNCTION_ARGS)
```
## Detailed Description
This function is the finalization step of the  aggregate operation in PostgreSQL. It processes the accumulated state from the aggregate's transition function and produces the final JSON array result. The function validates that it's being called in the proper aggregate context, handles NULL states (which occur when no rows were processed), and completes the JSON array by appending a closing bracket to the accumulated string.

The function follows PostgreSQL's aggregate function convention where a NULL result is returned when no input rows were processed, which is the standard behavior for aggregates.

## Parameters / Member Variables
- : Standard PostgreSQL function call information containing:
  - : Function call context information
  - Argument 0:  - Pointer to the aggregate's accumulated state (internal type)

## Dependencies
- Functions called/Symbols referenced:
  - : Validates that the function is called in proper aggregate context
  - : State structure containing accumulated JSON data
  - : Concatenates the closing bracket "]" to complete the JSON array
  - : Returns the final text result
  - : Checks if the input argument is NULL
  - : Retrieves the state pointer from function arguments
  - : Returns NULL when no input rows were processed

- Called from (representative examples):
  - PostgreSQL aggregate execution framework (no direct callers found in codebase)

## Notes and Other Information
- This function cannot be called directly due to its internal-type argument; it's only invoked by PostgreSQL's aggregate execution system
- The function assumes the JSON array opening bracket "[" was already added during the aggregate's initialization or transition phases
- Returns a properly formed JSON array text value that can be consumed by PostgreSQL's JSON processing functions
- Part of PostgreSQL's JSON aggregate functionality that allows converting multiple rows into a single JSON array