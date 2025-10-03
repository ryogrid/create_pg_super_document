# make_placeholder_expr

## Location
[src/backend/optimizer/util/placeholder.c:54-82](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/placeholder.c#L54-L82)

## Overview
Creates a PlaceHolderVar node for a given expression, which is used in PostgreSQL's query optimizer to represent expressions that need to be computed at specific query levels and locations within the join tree.

## Definition

```c
PlaceHolderVar *
make_placeholder_expr(PlannerInfo *root, Expr *expr, Relids phrels)
```
## Detailed Description
The  function constructs a PlaceHolderVar node that wraps an expression with metadata about where it should be evaluated in the query plan. PlaceHolderVars are essential for correctly handling expressions that need to be computed at specific levels in the join tree, particularly when dealing with outer joins and subqueries. The function initializes a new PlaceHolderVar with a unique identifier and sets up the basic structure, leaving some fields for later adjustment by the caller.

The function operates at the global level (root->glob) to ensure it doesn't interfere with query-level specific planning information, since the PHV may be used across different query levels.

## Parameters / Member Variables
- `*root`: PlannerInfo structure containing global planning state and context
- `*expr`: The expression to be wrapped in a PlaceHolderVar
- `phrels`: Relids representing the syntactic location (set of relation IDs) where this expression should be attributed
## Dependencies
- Functions called/Symbols referenced:
  - makeNode (for creating PlaceHolderVar)
  - [PlaceHolderVar](../P/PlaceHolderVar.md) (node type)
  - [PlaceHolderInfo](../P/PlaceHolderInfo.md) (related structure)
- Called from (representative examples):
  - [pullup_replace_vars_callback](../p/pullup_replace_vars_callback.md) (in prepjointree.c)
  - [add_nullingrels_if_needed](../a/add_nullingrels_if_needed.md) (in var.c)

## Notes and Other Information
- The caller is responsible for adjusting phlevelsup and phnullingrels fields as needed
- The function assigns a unique identifier (phid) by incrementing root->glob->lastPHId
- Initial values: phnullingrels is set to NULL, phlevelsup is set to 0
- The function only touches root->glob to avoid interfering with query-level planning
- PlaceHolderVars are crucial for maintaining correct expression evaluation semantics in complex queries involving outer joins

## Simplified Source

```c
PlaceHolderVar *
make_placeholder_expr(PlannerInfo *root, Expr *expr, Relids phrels)
{
    // Create new PlaceHolderVar node
    PlaceHolderVar *phv = makeNode(PlaceHolderVar);

    // Set the expression and location info
    phv->phexpr = expr;
    phv->phrels = phrels;

    // Initialize default values (caller may adjust later)
    phv->phnullingrels = NULL;
    phv->phlevelsup = 0;

    // Assign unique identifier
    phv->phid = ++(root->glob->lastPHId);

    return phv;
}
```