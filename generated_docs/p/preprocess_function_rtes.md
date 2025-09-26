# preprocess_function_rtes

## Location
[src/backend/optimizer/prep/prepjointree.c:887-933](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/prep/prepjointree.c#L887-L933)

## Overview
Constant-simplifies FUNCTION RTEs in the FROM clause and attempts to inline set-returning functions by converting them to subqueries for optimization purposes.

## Definition

```c
structure doesn't escape the planner.
				 */
				rte->funcordinality = false;
```
## Detailed Description
This function processes all FUNCTION range table entries (RTEs) in a query's FROM clause by performing two key optimizations:

1. **Constant Simplification**: Applies constant-folding optimizations to function expressions to evaluate any compile-time constant expressions.

2. **Function Inlining**: For set-returning functions that contain simple SELECT statements, converts the RTE_FUNCTION entry into an RTE_SUBQUERY entry that directly exposes the SELECT. This transformation reduces executor overhead and enables further subquery optimization techniques like pull-up operations.

The function must run before subquery optimization begins to ensure that inlined subqueries benefit from subsequent optimization passes. However, it runs after  to allow inlining of functions used within SubLink subselects.

The constant simplification step ensures that  will detect constants when present and guarantees that all FUNCTION RTEs are const-simplified, allowing  to skip redundant work.

## Parameters / Member Variables
- : PlannerInfo structure containing the query tree and optimization context

## Dependencies
- Functions called/Symbols referenced:
  -  - Performs constant folding on function expressions
  -  - Attempts to inline set-returning functions as subqueries
  -  - [Range](../R/Range.md) table entry type constant for function calls
  -  - [Range](../R/Range.md) table entry type constant for subqueries

- Called from (representative examples):
  -  - Main subquery planning entry point
  -  - During subquery pull-up processing

## Notes and Other Information
- This function modifies the query tree structure in place, converting RTE_FUNCTION entries to RTE_SUBQUERY entries when inlining is successful
- The original  field is temporarily preserved even after conversion to RTE_SUBQUERY to support  operations, and is later cleared in 
- Function inlining is only performed when it's safe and beneficial, as determined by 
- This preprocessing step is critical for enabling subsequent subquery optimization techniques on inlined functions