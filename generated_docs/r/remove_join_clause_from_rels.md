# remove_join_clause_from_rels

## Location
[src/backend/optimizer/util/joininfo.c:161-183](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/joininfo.c#L161-L183)

## Overview
Removes a join restriction clause from all the joininfo lists it was added to, reversing the effect of add_join_clause_to_rels when a relation is determined to not need joining.

## Definition
```c
void remove_join_clause_from_rels(PlannerInfo *root, RestrictInfo *restrictinfo, Relids join_relids)
```

## Detailed Description
This function serves as the inverse operation to add_join_clause_to_rels, removing a specific RestrictInfo node from the joininfo lists of all participating base relations. It is typically used when the query optimizer discovers that a relation does not need to be joined at all, requiring cleanup of previously distributed join clauses.

The function operates by iterating through all relation IDs in the join_relids bitmap and removing the specified restrictinfo from each base relation's joininfo list. It uses pointer comparison for efficiency since the same RestrictInfo node instance was shared across all lists during the original addition.

The function includes an assertion to verify that the restrictinfo exists in each relation's joininfo list before attempting removal, helping catch potential inconsistencies in join clause management.

## Parameters / Member Variables
- `root`: PlannerInfo structure containing global planning information
- `restrictinfo`: RestrictInfo node describing the join clause to be removed
- `join_relids`: Bitmap set of relation IDs from which the join clause should be removed

## Dependencies
- Functions called/Symbols referenced:
  - [bms_next_member](../b/bms_next_member.md)
  - [find_base_rel_ignore_join](../f/find_base_rel_ignore_join.md)
  - [list_member_ptr](../l/list_member_ptr.md)
  - [list_delete_ptr](../l/list_delete_ptr.md)
- Called from (representative examples):
  - [remove_rel_from_query](remove_rel_from_query.md)

## Notes and Other Information
- This function reverses the effect of add_join_clause_to_rels
- Only operates on base relations, consistent with the original addition logic
- Uses pointer comparison for efficient RestrictInfo identification and removal
- Includes assertion checking to ensure consistency in join clause management
- Typically used when query optimization determines a relation can be eliminated
- Located in src/backend/optimizer/util/joininfo.c:161-183