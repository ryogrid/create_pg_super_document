# find_nonnullable_rels

## Location
src/backend/optimizer/util/clauses.c: 1456 - 1461

## Overview
The `find_nonnullable_rels` function determines which base relations are forced to be nonnullable by a given clause, identifying relations that cannot be all-NULL rows when the clause evaluates to TRUE.

## Definition
```c
Relids find_nonnullable_rels(Node *clause)
```

## Detailed Description
This function analyzes a Boolean expression (clause) to determine which base relations must be non-null for the clause to potentially return TRUE. It serves as a crucial component in PostgreSQL's outer join optimization logic, helping the optimizer understand when outer joins can be converted to inner joins or when join elimination is possible.

The function operates under the assumption that the input is a Boolean expression that has been AND/OR flattened and converted to implicit-AND format. It identifies relations that would cause the entire clause to evaluate to FALSE or NULL if any of those relations contained an all-NULL row.

The analysis is deliberately conservative - it's acceptable to err on the side of caution by not detecting some nonnullable relations, but it must never incorrectly identify a relation as nonnullable when it might actually accept NULL values.

This function differs from `find_nonnullable_vars()` in that it focuses on entire relations rather than individual variables, and it specifically handles cases like "t1.v1 IS NOT NULL OR t1.v2 IS NOT NULL" which prove the entire t1 row cannot be all-NULL, even though individual columns might be.

## Parameters / Member Variables
- `clause`: A Node pointer representing the Boolean expression to be analyzed for nonnullable relations

## Dependencies
- Functions called/Symbols referenced:
  - [find_nonnullable_rels_walker](find_nonnullable_rels_walker.md)
- Called from (representative examples):
  - [make_outerjoininfo](../m/make_outerjoininfo.md)
  - [reduce_outer_joins_pass2](../r/reduce_outer_joins_pass2.md)
  - WindowFuncLists

## Notes and Other Information
- Returns a Relids bitmapset containing the OIDs of relations that are forced to be nonnullable
- The function serves as a wrapper that calls the actual implementation in `find_nonnullable_rels_walker` with top_level=true
- Essential for outer join optimization and join elimination in the PostgreSQL query planner
- Used in conjunction with outer join reduction logic to simplify query plans
- Part of the query optimization infrastructure that helps convert outer joins to more efficient inner joins when possible
- Located in src/backend/optimizer/util/clauses.c:1456-1461