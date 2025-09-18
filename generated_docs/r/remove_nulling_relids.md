# remove_nulling_relids

## Location
src/backend/rewrite/rewriteManip.c: 1238 - 1253

## Overview
remove_nulling_relids traverses an expression tree and removes specified relation IDs from the nulling relation sets of Vars and PlaceHolderVars, with exceptions for variables belonging to certain protected relations.

## Definition
```c
Node *remove_nulling_relids(Node *node,
                           const Bitmapset *removable_relids,
                           const Bitmapset *except_relids)
```

## Detailed Description
This function is the counterpart to add_nulling_relids and plays a crucial role in PostgreSQL's outer join optimization. It traverses an expression or query tree and removes the specified relation IDs from the varnullingrels and phnullingrels fields of Vars and PlaceHolderVars respectively.

The function provides fine-grained control through the except_relids parameter, which allows certain variables to be protected from nulling relation removal. Variables belonging to relations listed in except_relids will not have their nulling relations modified, even if they would otherwise match the removal criteria.

This functionality is essential during query optimization phases where the optimizer determines that certain outer joins can be simplified or where nulling relations are no longer needed due to query transformations. The function uses the same tree mutation framework as add_nulling_relids to ensure consistent and thorough processing of the entire expression tree.

## Parameters / Member Variables
- `node`: The root Node of the expression or query tree to be processed
- `removable_relids`: A Bitmapset containing the relation IDs to be removed from nulling relation sets
- `except_relids`: A Bitmapset containing relation IDs of variables that should be exempt from nulling relation removal

## Dependencies
- Functions called/Symbols referenced:
  - query_or_expression_tree_mutator (tree traversal framework)
  - [remove_nulling_relids_mutator](remove_nulling_relids_mutator.md) (actual mutation callback function)
- Structures used:
  - remove_nulling_relids_context (context structure containing removable_relids, except_relids, and sublevels_up)
- Called from (representative examples):
  - [reconsider_full_join_clause](reconsider_full_join_clause.md) (in optimizer/path/equivclass.c)
  - [deconstruct_distribute_oj_quals](../d/deconstruct_distribute_oj_quals.md) (in optimizer/plan/initsplan.c)
  - [reduce_outer_joins](reduce_outer_joins.md) (in optimizer/prep/prepjointree.c)
  - [remove_useless_result_rtes](remove_useless_result_rtes.md) (in optimizer/prep/prepjointree.c)
  - [have_partkey_equi_join](../h/have_partkey_equi_join.md) (in optimizer/util/relnode.c)

## Notes and Other Information
- Located in src/backend/rewrite/rewriteManip.c:1238-1253
- Works in conjunction with remove_nulling_relids_mutator which performs the actual variable modifications
- The except_relids mechanism allows selective protection of certain variables during the removal process
- Essential for outer join reduction and optimization where nulling relations become unnecessary
- Part of PostgreSQL's comprehensive outer join handling system that maintains semantic correctness during query transformation
- Used extensively during query optimization to clean up nulling relation information when outer joins are eliminated or simplified