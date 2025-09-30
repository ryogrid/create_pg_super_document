# pull_up_subqueries

## Location
[src/backend/optimizer/prep/prepjointree.c:934-977](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/prep/prepjointree.c#L934-L977)

## Overview
Entry point function that identifies and pulls up subqueries from the range table into the parent query's jointree to enable better optimization opportunities.

## Definition

```c
structure while recursing, in the following sense:
 * whenever we recurse, all qual expressions in the tree must be reachable
 * from the top level, in case the recursive call needs to modify them.
 *
 * Notice also that we can't turn pullup_replace_vars loose on the whole
 * jointree, because it'd return a mutated copy of the tree;
```
## Detailed Description
This function serves as the main entry point for the subquery pull-up optimization process. It examines subqueries in the range table and attempts to merge them directly into the parent query's join tree when beneficial and safe to do so.

The function handles two main types of subquery optimizations:

1. **Simple Subquery Pull-up**: Subqueries without special features (like grouping, aggregation, DISTINCT, etc.) can be flattened by merging their FROM clause into the parent query's jointree.

2. **UNION ALL Conversion**: Simple UNION ALL structures can be converted into "append relations", which are more efficiently processed by the executor.

The function operates on the assumption that the top level of the jointree is always a FromExpr node and preserves this invariant. It delegates the actual work to  which performs the recursive traversal and transformation of the query tree.

## Parameters / Member Variables
- : PlannerInfo structure containing the query tree and planning context

## Dependencies
- Functions called/Symbols referenced:
  -  - Performs the recursive subquery pull-up processing
  -  - [Node](../N/Node.md) type representing FROM clause expressions
  -  - Macro for type checking nodes

- Called from (representative examples):
  -  - Main subquery planning function
  -  - During recursive subquery processing

## Notes and Other Information
- This function maintains the structural invariant that the top level of the jointree is always a FromExpr
- The actual pull-up logic is delegated to  which handles the complex recursive traversal
- Subquery pull-up is a critical optimization that can significantly improve query performance by reducing the number of subplan executions
- The function starts recursion with no containing join or appendrel context (NULL parameters)
- Pull-up operations must preserve query semantics while potentially rewriting the query structure significantly

## Simplified Source

```c
void pull_up_subqueries(PlannerInfo *root) {
    // Verify we start with a FromExpr at the top level
    Assert(IsA(root->parse->jointree, FromExpr));

    // Recursively pull up subqueries starting from the root jointree
    // NULL parameters indicate no containing join or appendrel context
    root->parse->jointree = (FromExpr *)
        pull_up_subqueries_recurse(root, (Node *) root->parse->jointree,
                                   NULL, NULL);

    // Ensure we still have a FromExpr after processing
    Assert(IsA(root->parse->jointree, FromExpr));
}
```