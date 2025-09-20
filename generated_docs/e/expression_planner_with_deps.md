# expression_planner_with_deps

## Location
[src/backend/optimizer/plan/planner.c:6685-6737](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/planner.c#L6685-L6737)

## Overview
Performs planner transformations on standalone expressions while tracking and returning dependency information for caching purposes.

## Definition

```c
Expr *
expression_planner_with_deps(Expr *expr,
							 List **relationOids,
							 List **invalItems)
```
## Detailed Description
The  function extends  by tracking dependencies of the transformed expression. It performs the same core transformations (constant folding, function call normalization, operator ID resolution) but additionally:

1. **Sets up planner context**: Creates dummy  and  structures to enable dependency tracking
2. **Tracks dependencies during evaluation**: Uses the planner context to collect relation OIDs and invalidation items during 
3. **Extracts additional dependencies**: Calls  to find any remaining dependencies in the final expression
4. **Returns dependency information**: Outputs lists of relation OIDs and plan invalidation items that affect the expression

This function is designed for scenarios where expressions need to be cached and the cache must be invalidated when dependencies change, such as in the plan cache system.

## Parameters / Member Variables
- : Input expression tree from the parser that needs to be transformed
- : Output parameter - list of relation OIDs that the expression depends on
- : Output parameter - list of PlanInvalItems for cache invalidation purposes

## Dependencies
- Functions called/Symbols referenced:
  -  - Planner global state structure type
  -  - Memory initialization utility  
  -  - Performs constant folding and function call normalization with dependency tracking
  -  - Resolves missing operator function IDs
  -  - Extracts additional dependencies from expression tree
- Called from (representative examples):
  -  - For cached expression retrieval in plan cache

## Notes and Other Information
- Essential for plan cache functionality where dependency tracking enables proper cache invalidation
- Creates dummy planner state structures to leverage existing dependency tracking infrastructure
- More expensive than  due to additional dependency analysis
- Used when expressions need to be cached beyond current query duration
- The dependency information enables proper cache invalidation when referenced objects change
- Similar core transformation logic to  but with comprehensive dependency tracking