# remove_nulling_relids_mutator

## Location
[src/backend/rewrite/rewriteManip.c:1254-1345](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/rewrite/rewriteManip.c#L1254-L1345)

## Overview
remove_nulling_relids_mutator is the worker function that performs the actual removal of specified relation IDs from the nulling relation sets of Vars and PlaceHolderVars during expression tree traversal, with protection for excepted relations.

## Definition
```c
static Node *remove_nulling_relids_mutator(Node *node,
                                          remove_nulling_relids_context *context)
```

## Detailed Description
This static function serves as the callback for the tree mutation framework used by remove_nulling_relids. It examines each node in the expression tree and performs specific operations based on the node type:

1. **For Var nodes**: If the variable is at the correct sublevel, is not in the except_relids set, and has overlapping nulling relations with the removable set, it creates a copy of the Var and uses bms_difference to remove the specified relation IDs from varnullingrels.

2. **For PlaceHolderVar nodes**: More complex handling is applied - the function checks that the PHV is at the correct sublevel and its phrels don't overlap with except_relids. It recursively processes the PHV's expression content, then updates both phnullingrels and phrels fields. Importantly, it preserves PHVs even when phnullingrels becomes empty, as PHVs serve critical roles in maintaining subexpression identity during optimization.

3. **For Query nodes**: Handles subqueries by incrementing the sublevels_up counter and recursively processing the subquery tree, then restoring the counter.

4. **For other node types**: Delegates to expression_tree_mutator for standard tree traversal.

The function includes sophisticated logic for PlaceHolderVars, ensuring that both the nulling relations and the underlying relations are properly updated while maintaining the PHV's structural integrity.

## Parameters / Member Variables
- `node`: The current node being processed in the tree traversal
- `context`: Pointer to remove_nulling_relids_context structure containing:
  - `removable_relids`: Relation IDs to remove from nulling relation sets
  - `except_relids`: Relations whose variables should be protected from modification
  - `sublevels_up`: Current nesting level for handling subqueries

## Dependencies
- Functions called/Symbols referenced:
  - [bms_is_member](../b/bms_is_member.md) (check if variable belongs to excepted relations)
  - [bms_overlap](../b/bms_overlap.md) (check if nulling relations overlap with removable set)
  - [bms_difference](../b/bms_difference.md) (remove relation IDs from bitmapsets)
  - bms_is_empty (verify phrels is not empty after modification)
  - copyObject (create deep copy of Var)
  - expression_tree_mutator (recursively process expression nodes and PHV contents)
  - query_tree_mutator (recursively process Query nodes)
- Called from:
  - [remove_nulling_relids](remove_nulling_relids.md) (primary entry point)
  - Recursively calls itself during tree traversal

## Notes and Other Information
- Located in src/backend/rewrite/rewriteManip.c:1254-1345
- Static function, only accessible within the same compilation unit
- Includes important design decision to preserve PlaceHolderVars even when phnullingrels becomes empty, due to their role in enforcing subexpression identity
- Updates both phnullingrels and phrels for PlaceHolderVars, ensuring consistency
- Uses bms_difference for safe removal of relation IDs from bitmapsets
- Includes Assert to verify that phrels remains non-empty after processing
- Critical component of outer join optimization and nulling relation cleanup in PostgreSQL's query planner