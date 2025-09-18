# range_contains_elem_support

## Location
src/backend/utils/adt/rangetypes.c: 2213 - 2246

## Overview
Planner support function for the range_contains_elem operator (@>), which determines if a range contains a specific element. This function provides query optimization support by simplifying expressions involving range containment checks.

## Definition
```c
Datum range_contains_elem_support(PG_FUNCTION_ARGS)
```

## Detailed Description
This function serves as a planner support function specifically for the @> operator when checking if a range contains an element. It implements query optimization logic by handling SupportRequestSimplify requests from the PostgreSQL query planner. When the planner encounters expressions involving range containment of elements, this function can suggest simplified alternatives that may be more efficient to execute.

The function examines the function call expression and attempts to find a simplified clause by calling find_simplified_clause with the original operand order (leftop, rightop). This is the complement to elem_contained_by_range_support, handling the inverse containment relationship.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument macro containing:
  - `rawreq`: A Node pointer representing the support request from the planner
  - The function expects a SupportRequestSimplify request type

## Dependencies
- Functions called/Symbols referenced:
  - `SupportRequestSimplify` (struct type for planner optimization requests)
  - `FuncExpr` (expression node representing function calls)
  - `lsecond` (macro to get second element from a list)
  - `[find_simplified_clause](../f/find_simplified_clause.md)` (function to find simplified expression alternatives)
- Called from:
  - No direct references found (likely registered as an operator support function)

## Notes and Other Information
- This is a planner support function, meaning it's called during query planning phase rather than execution
- The function specifically handles the @> (range contains element) operator
- Unlike elem_contained_by_range_support, this function maintains the original operand order when calling find_simplified_clause
- The function returns NULL if the request type is not SupportRequestSimplify
- Part of PostgreSQL's range type system for efficient range operations optimization
- Forms a complementary pair with elem_contained_by_range_support for bidirectional containment optimization