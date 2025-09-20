# is_degenerate_grouping

## Location
[src/backend/optimizer/plan/planner.c:3986-4006](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/planner.c#L3986-L4006)

## Overview
Determines whether the current query represents a degenerate grouping case that can be optimized with special handling.

## Definition

```c
static bool
is_degenerate_grouping(PlannerInfo *root)
```
## Detailed Description
This function identifies a specific optimization case in GROUP BY processing called "degenerate grouping." A degenerate grouping occurs when a query has either a HAVING clause or grouping sets, but lacks both aggregate functions and actual GROUP BY columns. This typically happens with empty grouping sets or when grouping is used purely for filtering purposes without aggregation.

The function checks for the specific combination of conditions:
1. The query has a HAVING qualifier OR contains grouping sets
2. AND the query has no aggregate functions 
3. AND the GROUP BY clause is empty

When these conditions are met, PostgreSQL can apply specialized optimizations since no actual grouping computation is needed - the query essentially becomes a filtered scan or produces a predetermined number of result rows (one per empty grouping set).

## Parameters / Member Variables
- : PlannerInfo structure containing the parsed query and planning context information

## Dependencies
- Functions called/Symbols referenced:
  - None (accesses only PlannerInfo and Query structure fields)
- Data structures accessed:
  - [Query](../Q/Query.md) fields: groupingSets, hasAggs, groupClause
  - [PlannerInfo](../P/PlannerInfo.md) fields: hasHavingQual
- Called from:
  - standard_qp_extra
  - [create_grouping_paths](../c/create_grouping_paths.md)

## Notes and Other Information
- This is a pure predicate function with no side effects, used for optimization path selection
- Degenerate grouping allows PostgreSQL to avoid the overhead of setting up grouping/aggregation machinery
- The most common case is queries with empty grouping sets: 
- Another case is HAVING-only queries without GROUP BY: 
- When true, the query planner will call create_degenerate_grouping_paths instead of create_ordinary_grouping_paths
- This optimization can provide significant performance benefits by avoiding unnecessary grouping operations