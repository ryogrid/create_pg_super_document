# chooseScalarFunctionAlias

## Location
[src/backend/parser/parse_relation.c:1254-1293](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_relation.c#L1254-L1293)

## Overview
Selects the appropriate column alias for a function in a function RTE when the function returns a scalar type (not composite or RECORD).

## Definition

```c
static char *
chooseScalarFunctionAlias(Node *funcexpr, char *funcname,
						  Alias *alias, int nfuncs)
```
## Detailed Description
This function implements a priority-based algorithm to determine the best column name for scalar functions in FROM clauses. It first attempts to use the function's named OUT parameter if available, then falls back to using the RTE alias name if there's only one function and an alias is provided, and finally defaults to the function name itself. This ensures meaningful and predictable column naming for scalar function results.

## Parameters / Member Variables
- : The transformed expression tree for the function call (Node pointer)
- : The function name as determined by FigureColname
- : The user-supplied alias for the RTE, or NULL if none provided
- : The number of functions appearing in the function RTE

## Dependencies
- Functions called/Symbols referenced:
  - IsA (type checking macro)
  - [get_func_result_name](../g/get_func_result_name.md) (retrieves named OUT parameter)
  - [FuncExpr](../F/FuncExpr.md) (function expression node type)
- Called from (representative examples):
  - [addRangeTableEntryForFunction](../a/addRangeTableEntryForFunction.md)

## Notes and Other Information
- The chosen name may be overridden later if user-given aliases include column alias names
- Priority order: OUT parameter name > RTE alias name (single function only) > function name
- Only applies to scalar-returning functions; composite and RECORD types are handled elsewhere
- Part of the query parsing phase where function RTEs are being constructed
- Helps maintain consistency in column naming across different function invocation patterns