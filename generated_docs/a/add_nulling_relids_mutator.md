# add_nulling_relids_mutator

## Location
src/backend/rewrite/rewriteManip.c: 1165 - 1237

## Overview
add_nulling_relids_mutator is the worker function that performs the actual modification of Vars and PlaceHolderVars by adding specified relation IDs to their nulling relation sets during expression tree traversal.

## Definition
```c
static Node *add_nulling_relids_mutator(Node *node,
                                       add_nulling_relids_context *context)
```

## Detailed Description
This static function serves as the callback for the tree mutation framework used by add_nulling_relids. It examines each node in the expression tree and performs specific operations based on the node type:

1. **For Var nodes**: If the variable is at the correct sublevel (matching context->sublevels_up) and belongs to one of the target relations (or if target_relids is NULL), it creates a copy of the Var and updates its varnullingrels field by adding the specified relation IDs.

2. **For PlaceHolderVar nodes**: Similar logic applies, but uses the phrels field to check relation membership and updates phnullingrels. The PHV's expression content is not recursively modified, only the nulling relations are updated, reflecting the assumption that the PHV is evaluated at its original level before potentially being nulled.

3. **For Query nodes**: Handles subqueries by incrementing the sublevels_up counter and recursively processing the subquery tree, then restoring the counter.

4. **For other node types**: Delegates to expression_tree_mutator for standard tree traversal.

The function ensures proper handling of nested subqueries by tracking the sublevel depth, which is crucial for correctly identifying variables that should be modified.

## Parameters / Member Variables
- `node`: The current node being processed in the tree traversal
- `context`: Pointer to add_nulling_relids_context structure containing:
  - `target_relids`: Relations whose variables should be modified (NULL means all level-zero variables)
  - `added_relids`: Relation IDs to add to nulling relation sets
  - `sublevels_up`: Current nesting level for handling subqueries

## Dependencies
- Functions called/Symbols referenced:
  - bms_is_member (check if relation is in target set)
  - bms_union (combine relation ID bitmapsets)
  - bms_overlap (check if PHV relations overlap with targets)
  - copyObject (create deep copy of Var)
  - makeNode (create new PlaceHolderVar)
  - query_tree_mutator (recursively process Query nodes)
  - expression_tree_mutator (recursively process other expression nodes)
- Called from:
  - add_nulling_relids (primary entry point)
  - Recursively calls itself during tree traversal

## Notes and Other Information
- Located in src/backend/rewrite/rewriteManip.c:1165-1237
- Static function, only accessible within the same compilation unit
- Uses different copying strategies for Var (copyObject) vs PlaceHolderVar (makeNode + memcpy) nodes
- The sublevel tracking mechanism ensures that variables from different query levels are handled correctly
- For PlaceHolderVars, only the nulling relations are modified, not the contained expression, preserving the PHV's evaluation semantics
- Critical component of PostgreSQL's outer join nulling infrastructure