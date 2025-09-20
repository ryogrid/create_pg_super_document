# postquel_sub_params

## Location
[src/backend/executor/functions.c:931-985](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/functions.c#L931-L985)

## Overview
Builds a ParamListInfo array representing the current function arguments, converting FunctionCallInfo parameters into a format suitable for query execution within SQL functions.

## Definition

```c
structure.  (Examining the parse trees is not good enough,
			 * because of possible function inlining during planning.)
			 */
			prm->isnull = fcinfo->args[i].isnull;
```
## Detailed Description
postquel_sub_params is responsible for parameter substitution in SQL functions. It takes the arguments passed to a SQL function call and converts them into a ParamListInfo structure that can be used during query execution. The function handles parameter caching by reusing existing ParamListInfo structures when possible, and includes special handling for expanded datums by forcing them to read-only status to prevent mutation issues when parameters are referenced multiple times within the function. Each parameter's value, null status, type, and flags are properly set up for use by the query executor.

## Parameters / Member Variables
- : Pointer to the SQLFunctionCache containing cached function information and parameter structures
- : FunctionCallInfo containing the actual argument values passed to the SQL function

## Dependencies
- Functions called/Symbols referenced:
  - [makeParamList](../m/makeParamList.md)
  - MakeExpandedObjectReadOnly
  - [get_typlen](../g/get_typlen.md)
- Called from (representative examples):
  - [fmgr_sql](../f/fmgr_sql.md)

## Notes and Other Information
- Creates or reuses ParamListInfo structures to avoid repeated allocation overhead
- Forces expanded datums to read-only status to prevent mutation side effects when parameters are referenced multiple times
- Handles null arguments appropriately by setting the isnull flag
- If there are no arguments (nargs == 0), sets fcache->paramLI to NULL
- The function includes detailed comments explaining the rationale for making expanded objects read-only to prevent parameter mutation issues