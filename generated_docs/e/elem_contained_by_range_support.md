# elem_contained_by_range_support

## Location
[src/backend/utils/adt/rangetypes.c:2187-2212](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/rangetypes.c#L2187-L2212)

## Overview
Planner support function for the elem_contained_by_range operator (<@), which determines if an element is contained within a range type. This function provides query optimization support by simplifying expressions involving element containment checks.

## Definition

```c
Datum
elem_contained_by_range_support(PG_FUNCTION_ARGS)
```
## Detailed Description
This function serves as a planner support function specifically for the <@ operator when checking if an element is contained by a range. It implements query optimization logic by handling SupportRequestSimplify requests from the PostgreSQL query planner. When the planner encounters expressions involving element containment in ranges, this function can suggest simplified alternatives that may be more efficient to execute.

The function examines the function call expression and attempts to find a simplified clause by calling find_simplified_clause with swapped operand order (rightop, leftop), which allows for potential query optimization through clause reordering or transformation.

## Parameters / Member Variables
- : Standard PostgreSQL function argument macro containing:
  - : A Node pointer representing the support request from the planner
  - The function expects a SupportRequestSimplify request type

## Dependencies
- Functions called/Symbols referenced:
  -  (struct type for planner optimization requests)
  -  (expression node representing function calls)
  -  (macro to get second element from a list)
  -  (function to find simplified expression alternatives)
- Called from:
  - No direct references found (likely registered as an operator support function)

## Notes and Other Information
- This is a planner support function, meaning it's called during query planning phase rather than execution
- The function specifically handles the <@ (element contained by range) operator
- It swaps operand order when calling find_simplified_clause, which suggests it's looking for equivalent expressions with different operand arrangements
- The function returns NULL if the request type is not SupportRequestSimplify
- Part of PostgreSQL's range type system for efficient range operations optimization