# is_safe_append_member

## Location
[src/backend/optimizer/prep/prepjointree.c:2143-2190](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/prep/prepjointree.c#L2143-L2190)

## Overview
This function checks whether a subquery that is a leaf of a UNION ALL appendrel is safe to pull up into the parent query.

## Definition
```c
static bool is_safe_append_member(Query *subquery)
```

## Detailed Description
The is_safe_append_member function determines if a subquery can be safely pulled up as part of a UNION ALL optimization. This is a critical safety check in PostgreSQL's appendrel (append relation) optimization infrastructure.

The function enforces two key safety requirements:

1. **Single RTE Constraint**: The subquery's jointree must contain exactly one RangeTblEntry (RTE), though it may be buried within multiple levels of FromExpr nodes. This constraint exists because the AppendRelInfo data structure assumes a one-to-one mapping between appendrel members and base relations.

2. **No WHERE Clauses**: The subquery cannot have any WHERE quals at any level of its jointree, because there is no appropriate place to relocate these conditions within an appendrel structure.

The function handles two special cases:
- **Empty Jointree**: If the jointree is completely empty (no fromlist and no quals), pullup is safe because pull_up_simple_subquery will insert a single RTE_RESULT RTE.
- **Nested FromExpr**: The function traverses through multiple levels of FromExpr nodes to find the underlying RTE, checking for quals at each level.

The validation process walks down the jointree hierarchy, ensuring that each FromExpr level has exactly one child in its fromlist and no quals, until it reaches a RangeTblRef leaf node.

## Parameters / Member Variables
- `subquery`: Query structure representing the subquery to be checked for appendrel pullup safety

## Dependencies
- Functions called/Symbols referenced:
  - FromExpr
  - RangeTblRef
- Called from:
  - [pull_up_subqueries_recurse](../p/pull_up_subqueries_recurse.md)
  - [pull_up_simple_subquery](../p/pull_up_simple_subquery.md)

## Notes and Other Information
- This function is specifically designed for UNION ALL appendrel optimization scenarios
- The single RTE constraint is imposed by the AppendRelInfo data structure design
- The no-WHERE-quals restriction is a limitation of the appendrel framework
- An alternative implementation could use get_relids_in_jointree() to check for singleton sets, but the WHERE clause check requires the current traversal approach
- The function is related to fix_append_rel_relids() in terms of coding patterns
- Located in src/backend/optimizer/prep/prepjointree.c:2143-2190