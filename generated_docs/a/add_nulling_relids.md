# add_nulling_relids

## Location
src/backend/rewrite/rewriteManip.c: 1149 - 1164

## Overview
add_nulling_relids traverses an expression tree and adds specified relation IDs to the nulling relation sets of Vars and PlaceHolderVars that belong to target relations, implementing outer join nulling semantics.

## Definition
```c
Node *add_nulling_relids(Node *node,
                        const Bitmapset *target_relids,
                        const Bitmapset *added_relids)
```

## Detailed Description
This function is a key component of PostgreSQL's outer join handling mechanism. It walks through an expression tree (which can be a query tree or any expression node) and modifies Vars and PlaceHolderVars that belong to any of the specified target relations by adding the given relation IDs to their nulling relation sets (varnullingrels and phnullingrels fields).

The function uses the query_or_expression_tree_mutator framework to traverse the tree systematically. When target_relids is NULL, the function processes all level-zero Vars and PlaceHolderVars regardless of which relation they belong to. The nulling relation information is crucial for correctly implementing outer join semantics, where certain relations may produce NULL values when no matching rows are found.

This function is primarily used during query planning and rewriting phases to propagate nulling relation information as the query tree is transformed and optimized.

## Parameters / Member Variables
- `node`: The root Node of the expression or query tree to be processed
- `target_relids`: A Bitmapset containing the relation IDs of relations whose Vars/PHVs should be modified; if NULL, all level-zero variables are processed
- `added_relids`: A Bitmapset containing the relation IDs to be added to the nulling relation sets of target variables

## Dependencies
- Functions called/Symbols referenced:
  - query_or_expression_tree_mutator (tree traversal framework)
  - [add_nulling_relids_mutator](add_nulling_relids_mutator.md) (actual mutation callback function)
- Structures used:
  - [add_nulling_relids_context](add_nulling_relids_context.md) (context structure containing target_relids, added_relids, and sublevels_up)
- Called from (representative examples):
  - [deconstruct_distribute_oj_quals](../d/deconstruct_distribute_oj_quals.md) (in optimizer/plan/initsplan.c)
  - [transform_MERGE_to_join](../t/transform_MERGE_to_join.md) (in optimizer/prep/prepjointree.c)
  - [pullup_replace_vars_callback](../p/pullup_replace_vars_callback.md) (in optimizer/prep/prepjointree.c)

## Notes and Other Information
- Located in src/backend/rewrite/rewriteManip.c:1149-1164
- Works in conjunction with add_nulling_relids_mutator which performs the actual variable modifications
- The context structure tracks sublevels_up to handle nested subqueries correctly
- This function is essential for maintaining correct outer join semantics throughout query optimization
- Part of PostgreSQL's sophisticated outer join handling that ensures proper NULL propagation in complex join scenarios