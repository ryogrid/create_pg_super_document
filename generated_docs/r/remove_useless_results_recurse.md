# remove_useless_results_recurse

## Location
[src/backend/optimizer/prep/prepjointree.c:3500-3770](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/prep/prepjointree.c#L3500-L3770)

## Overview
The recursive implementation function that processes the join tree to remove useless RTE_RESULT RTEs and optimize single-child FromExprs, handling different join types with specific optimization rules.

## Definition
```c
static Node *remove_useless_results_recurse(PlannerInfo *root, Node *jtnode, Node **parent_quals, Relids *dropped_outer_joins)
```

## Detailed Description
This function is the core recursive worker for remove_useless_result_rtes(). It traverses the join tree depth-first, applying optimizations based on the node type encountered:

**For RangeTblRef nodes**: No immediate processing possible, returned as-is.

**For FromExpr nodes**: 
- Recursively processes all children in the fromlist
- Removes RTE_RESULT children that have siblings and no dependent PlaceHolderVars
- Elides single-child FromExprs when safe (no quals or quals can be pushed to parent)
- Merges child quals upward when semantically valid

**For JoinExpr nodes**: Applies join-type-specific optimizations:
- **INNER joins**: Can remove either side if it's RTE_RESULT, replacing with the other side
- **LEFT joins**: Can remove RTE_RESULT from RHS, potentially strength-reducing to inner join
- **SEMI joins**: Can convert to filter on LHS when RHS is RTE_RESULT
- **FULL/ANTI joins**: No special optimizations applied

The function carefully handles PlaceHolderVar dependencies to ensure they remain evaluable after transformations.

## Parameters / Member Variables
- : PlannerInfo containing the query tree and related metadata
- : Current join tree node being processed
- : Pointer to parent node's quals list for merging child quals upward (NULL if merging not allowed)
- : Output parameter collecting RT indexes of removed outer-join nodes

## Dependencies
- Functions called/Symbols referenced:
  - [get_result_relid](../g/get_result_relid.md)
  - [find_dependent_phvs_in_jointree](../f/find_dependent_phvs_in_jointree.md)
  - [find_dependent_phvs](../f/find_dependent_phvs.md)
  - [remove_result_refs](remove_result_refs.md)
  - foreach_delete_current
  - [bms_add_member](../b/bms_add_member.md)
  - [bms_next_member](../b/bms_next_member.md)
  - [list_concat](../l/list_concat.md)
  - [makeFromExpr](../m/makeFromExpr.md)
  - nodeTag (for error handling)
  - JOIN_INNER, JOIN_LEFT, JOIN_SEMI, JOIN_FULL, JOIN_ANTI (constants)

- Called from (representative examples):
  - [remove_useless_result_rtes](remove_useless_result_rtes.md) (top-level caller)
  - [remove_useless_results_recurse](remove_useless_results_recurse.md) (recursive self-calls)

## Notes and Other Information
- This is a static function, only accessible within prepjointree.c
- Uses recursive descent to process the entire join tree structure
- Handles qual merging carefully to preserve semantics - inner joins allow bidirectional qual absorption, left joins allow RHS-to-current and LHS-to-parent merging
- The parent_quals parameter enables elimination of single-child FromExprs by allowing their quals to be pushed upward
- Maintains dropped_outer_joins set for later cleanup of nulling relation references
- Error handling for unrecognized node types and join types (JOIN_RIGHT should be eliminated by this point)
- Part of PostgreSQL's query optimization pipeline, specifically the join tree preprocessing phase