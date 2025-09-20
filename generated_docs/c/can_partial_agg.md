# can_partial_agg

## Location
[src/backend/optimizer/plan/planner.c:7663-7704](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/planner.c#L7663-L7704)

## Overview
Determines whether partial grouping and/or aggregation operations can be safely performed in a parallel execution context by validating query structure and aggregate function compatibility.

## Definition

```c
static bool
can_partial_agg(PlannerInfo *root)
```
## Detailed Description
This function serves as a gatekeeper for parallel aggregation by performing essential feasibility checks. It validates several critical conditions that must be met for safe parallel execution:

1. **Operation requirements**: Ensures the query has either aggregate functions (hasAggs) or GROUP BY clauses, as these are prerequisites for meaningful parallel aggregation
2. **GROUPING SETS limitation**: Rejects queries with GROUPING SETS since PostgreSQL doesn't support parallel execution of complex grouping sets operations
3. **Aggregate function compatibility**: Validates that all aggregate functions support partial aggregation modes by checking for non-partial or non-serial aggregates
4. **Serial execution detection**: Identifies aggregates that require serial execution and cannot be split across parallel workers

The function implements a conservative approach, returning false whenever any condition suggests parallel aggregation might produce incorrect results or encounter execution difficulties.

## Parameters / Member Variables
- : PlannerInfo containing parsed query information, aggregate function flags, and other planning context

## Dependencies
- Functions called/Symbols referenced:
  - (No external function calls - uses only direct field access)
- Called from (representative examples):
  - [create_grouping_paths](create_grouping_paths.md)
  - standard_qp_extra

## Notes and Other Information
- Returns true only when all conditions for safe parallel aggregation are satisfied
- The function is deliberately conservative to ensure correctness over performance
- Key flags checked: hasAggs, groupClause, groupingSets, hasNonPartialAggs, hasNonSerialAggs  
- This check prevents the creation of partial aggregation paths that would fail during execution
- Essential for determining whether to create UPPERREL_PARTIAL_GROUP_AGG relations
- Part of PostgreSQL's broader parallel query execution framework
- Location: src/backend/optimizer/plan/planner.c:7663-7704