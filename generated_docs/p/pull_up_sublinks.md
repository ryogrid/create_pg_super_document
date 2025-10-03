# pull_up_sublinks

## Location
[src/backend/optimizer/prep/prepjointree.c:453-479](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/prep/prepjointree.c#L453-L479)

## Overview
Attempts to pull up ANY and EXISTS SubLinks to be treated as semijoins or anti-semijoins for query optimization.

## Definition

```c
void
pull_up_sublinks(PlannerInfo *root)
```
## Detailed Description
This function performs an important query optimization by transforming SubLink expressions (ANY and EXISTS clauses) into semijoin or anti-semijoin operations. This transformation can significantly improve query execution performance by allowing the optimizer to consider additional join strategies and access paths.

The optimization works by:
1. Identifying SubLink expressions that can be safely transformed
2. Pulling up the sub-SELECT to become a rangetable entry
3. Converting the implied comparisons into semijoin or anti-semijoin conditions

However, this optimization has important restrictions:
- **Location restriction**: Only works at the top level of WHERE or JOIN/ON clauses, because the NULL semantics of ANY/EXISTS cannot be preserved at arbitrary expression depths
- **Outer join restriction**: In outer join ON clauses, the sublink must be degenerate (reference only the nullable side) to be eligible for transformation
- **NULL handling**: The transformation must preserve the original NULL/FALSE semantics of the sublink

The function recursively searches through the query's jointree to find and transform eligible sublinks, stopping at non-AND expressions since quals are not yet in implicit-AND format at this stage.

## Parameters / Member Variables
- `*root`: PlannerInfo structure containing the query tree and optimization context
## Dependencies
- Functions called/Symbols referenced:
  - [pull_up_sublinks_jointree_recurse](pull_up_sublinks_jointree_recurse.md)
  - [makeFromExpr](../m/makeFromExpr.md)
  - list_make1
  - IsA macro
  - [FromExpr](../F/FromExpr.md) type
- Called from (representative examples):
  - [subquery_planner](../s/subquery_planner.md) (in src/backend/optimizer/plan/planner.c:715)
  - [pull_up_simple_subquery](pull_up_simple_subquery.md) (in src/backend/optimizer/prep/prepjointree.c:1198)

## Notes and Other Information
- Must run before preprocess_expression() since quals are not yet in reduced implicit-AND format
- Handles explicit AND clauses recursively, stopping at non-AND items
- Always ensures root->parse->jointree remains a FromExpr by wrapping bare RangeTblRef or JoinExpr results
- The transformation enables better join planning by exposing more join relationships to the optimizer
- Critical for performance of queries with correlated subqueries that can be decorrelated
- Works in conjunction with pull_up_sublinks_jointree_recurse for the actual transformation logic

## Simplified Source

```c
void pull_up_sublinks(PlannerInfo *root) {
    Node *jtnode;
    Relids relids;

    // Recursively process the jointree to find and transform sublinks
    jtnode = pull_up_sublinks_jointree_recurse(root,
                                               (Node *) root->parse->jointree,
                                               &relids);

    // Ensure the result is always wrapped in a FromExpr
    if (IsA(jtnode, FromExpr))
        root->parse->jointree = (FromExpr *) jtnode;
    else
        root->parse->jointree = makeFromExpr(list_make1(jtnode), NULL);
}
```