# add_nulling_relids_mutator

## Location
[src/backend/rewrite/rewriteManip.c:1165-1237](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/rewrite/rewriteManip.c#L1165-L1237)

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
  - [bms_is_member](../b/bms_is_member.md) (check if relation is in target set)
  - [bms_union](../b/bms_union.md) (combine relation ID bitmapsets)
  - [bms_overlap](../b/bms_overlap.md) (check if PHV relations overlap with targets)
  - copyObject (create deep copy of Var)
  - makeNode (create new PlaceHolderVar)
  - query_tree_mutator (recursively process Query nodes)
  - expression_tree_mutator (recursively process other expression nodes)
- Called from:
  - [add_nulling_relids](add_nulling_relids.md) (primary entry point)
  - Recursively calls itself during tree traversal

## Notes and Other Information
- Located in src/backend/rewrite/rewriteManip.c:1165-1237
- Static function, only accessible within the same compilation unit
- Uses different copying strategies for Var (copyObject) vs PlaceHolderVar (makeNode + memcpy) nodes
- The sublevel tracking mechanism ensures that variables from different query levels are handled correctly
- For PlaceHolderVars, only the nulling relations are modified, not the contained expression, preserving the PHV's evaluation semantics
- Critical component of PostgreSQL's outer join nulling infrastructure

## Simplified Source
```c
static Node *add_nulling_relids_mutator(Node *node,
                                       add_nulling_relids_context *context) {
    if (node == NULL)
        return NULL;

    if (IsA(node, Var)) {
        Var *var = (Var *) node;

        // Check if this var should be modified
        if (var->varlevelsup == context->sublevels_up &&
            (context->target_relids == NULL ||
             bms_is_member(var->varno, context->target_relids))) {

            // Add new nulling relations to existing ones
            Relids newnullingrels = bms_union(var->varnullingrels,
                                            context->added_relids);

            // Copy var and update nulling relations
            var = copyObject(var);
            var->varnullingrels = newnullingrels;
            return (Node *) var;
        }
    }
    else if (IsA(node, PlaceHolderVar)) {
        PlaceHolderVar *phv = (PlaceHolderVar *) node;

        // Check if this PHV should be modified
        if (phv->phlevelsup == context->sublevels_up &&
            (context->target_relids == NULL ||
             bms_overlap(phv->phrels, context->target_relids))) {

            // Add new nulling relations
            Relids newnullingrels = bms_union(phv->phnullingrels,
                                            context->added_relids);

            // Shallow copy PHV and update nulling relations
            phv = makeNode(PlaceHolderVar);
            memcpy(phv, node, sizeof(PlaceHolderVar));
            phv->phnullingrels = newnullingrels;
            return (Node *) phv;
        }
    }
    else if (IsA(node, Query)) {
        // Handle subqueries
        Query *newnode;
        context->sublevels_up++;
        newnode = query_tree_mutator((Query *) node,
                                   add_nulling_relids_mutator,
                                   (void *) context, 0);
        context->sublevels_up--;
        return (Node *) newnode;
    }

    // Default: continue tree traversal
    return expression_tree_mutator(node, add_nulling_relids_mutator,
                                 (void *) context);
}
```