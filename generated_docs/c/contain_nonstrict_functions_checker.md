# contain_nonstrict_functions_checker

## Location
src/backend/optimizer/util/clauses.c: 999 - 1004

## Overview
A helper function that checks whether a given function is non-strict (i.e., can return non-NULL values even when given NULL inputs).

## Definition
```c
static bool contain_nonstrict_functions_checker(Oid func_id, void *context)
```

## Detailed Description
This function serves as a callback checker used by the PostgreSQL optimizer to identify non-strict functions within expression trees. A function is considered non-strict if it can produce a non-NULL result even when some or all of its input parameters are NULL. This checker simply negates the result of `func_strict()` to determine if a function is non-strict. The function is used as part of the expression tree walking mechanism to analyze whether an expression contains any non-strict functions, which is important for query optimization decisions such as predicate pushdown and NULL handling.

## Parameters / Member Variables
- `func_id`: The OID (Object ID) of the function to check for strictness
- `context`: A void pointer to context information (unused in this checker but required by the callback interface)

## Dependencies
- Functions called/Symbols referenced:
  - func_strict
- Called from (representative examples):
  - contain_nonstrict_functions_walker (via check_functions_in_node)

## Notes and Other Information
- This is a static helper function used exclusively within the clauses.c file
- The function implements a simple boolean callback interface compatible with check_functions_in_node
- Part of the PostgreSQL query optimizer's expression analysis infrastructure
- Located in src/backend/optimizer/util/clauses.c at lines 999-1004