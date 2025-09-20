# infer_arbiter_indexes

## Location
[src/backend/optimizer/util/plancat.c:705-977](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/plancat.c#L705-L977)

## Overview
Determines the unique indexes used to arbitrate speculative insertion for ON CONFLICT clauses by matching user-supplied inference specifications against available unique indexes.

## Definition

```c
List *
infer_arbiter_indexes(PlannerInfo *root)
```
## Detailed Description
This function implements the core logic for PostgreSQL's ON CONFLICT clause by identifying which unique indexes should be used for conflict detection during speculative insertion. It takes the inference specification from an OnConflictExpr and matches it against the unique indexes defined on the target relation.

The matching process involves several steps:
1. Parse arbiter elements (columns/expressions) from the ON CONFLICT clause
2. Build normalized representations of both plain attributes and expressions
3. Handle named constraint specifications by looking up the associated index
4. Iterate through all available indexes to find exact matches on columns/expressions
5. Verify collation and operator class compatibility via infer_collation_opclass_match
6. Ensure partial index predicates are implied by the WHERE clause
7. Return a list of matching index OIDs for conflict resolution

The function requires exact matches on indexed columns/expressions but allows flexible ordering. For partial indexes, the predicate must be logically implied by the ON CONFLICT WHERE clause.

## Parameters / Member Variables
- : PlannerInfo structure containing the parsed query with OnConflictExpr information

## Dependencies
- Functions called/Symbols referenced:
  - rt_fetch
  - table_open, table_close
  - [RelationGetIndexList](../R/RelationGetIndexList.md)
  - [index_open](index_open.md), index_close
  - [get_constraint_index](../g/get_constraint_index.md)
  - [infer_collation_opclass_match](infer_collation_opclass_match.md)
  - [RelationGetIndexExpressions](../R/RelationGetIndexExpressions.md)
  - [RelationGetIndexPredicate](../R/RelationGetIndexPredicate.md)
  - [predicate_implied_by](../p/predicate_implied_by.md)
  - [bms_add_member](../b/bms_add_member.md), bms_equal
  - [list_member](../l/list_member.md), list_difference
- Called from (representative examples):
  - make_modifytable

## Notes and Other Information
- Returns NIL for ON CONFLICT DO NOTHING without inference specification
- Does not consider indcheckxmin for candidate elimination (unlike get_relation_info)
- Supports both named constraints and inference element specifications
- Requires exact expression matching but allows flexible attribute ordering
- Validates that partial index predicates are implied by ON CONFLICT WHERE clause
- Raises errors for unsupported features like whole-row inference specifications
- Used specifically for UPSERT operations and conflict resolution in INSERT statements