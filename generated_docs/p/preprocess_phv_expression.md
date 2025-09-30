# preprocess_phv_expression

## Location
[src/backend/optimizer/plan/planner.c:1302-1334](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/planner.c#L1302-L1334)

## Overview
Preprocesses a PlaceHolderVar expression that has been pulled up from a LATERAL subquery to ensure proper expression handling in PostgreSQL's query planner.

## Definition

```c
Expr *
preprocess_phv_expression(PlannerInfo *root, Expr *expr)
```
## Detailed Description
This function is specifically designed to handle PlaceHolderVar expressions that arise from LATERAL subquery processing. When a LATERAL subquery references an output from another subquery, and that output must be wrapped in a PlaceHolderVar due to an intermediate outer join, the expression gets pushed down into the subquery and later pulled back up during find_lateral_references. Since this happens after subquery_planner has already preprocessed the expressions at the current query level, this function ensures that the pulled-up PlaceHolderVar expressions receive proper preprocessing.

The function serves as a specialized wrapper around preprocess_expression, using the EXPRKIND_PHV expression kind to indicate that this is a PlaceHolderVar context.

## Parameters / Member Variables
- : PlannerInfo structure containing planning context and state information
- : The PlaceHolderVar expression that needs to be preprocessed

## Dependencies
- Functions called/Symbols referenced:
  - [preprocess_expression](preprocess_expression.md)
  - EXPRKIND_PHV
- Called from (representative examples):
  - [extract_lateral_references](../e/extract_lateral_references.md)

## Notes and Other Information
- This function is part of the LATERAL subquery handling mechanism in PostgreSQL's optimizer
- The preprocessing occurs after the normal expression preprocessing phase due to the timing of LATERAL reference resolution
- Located in src/backend/optimizer/plan/planner.c:1302-1334
- The function is a thin wrapper that delegates the actual work to preprocess_expression with the appropriate expression kind

## Simplified Source

```c
Expr *
preprocess_phv_expression(PlannerInfo *root, Expr *expr)
{
    // Simple wrapper to preprocess PlaceHolderVar expressions
    // with the correct expression kind for LATERAL subquery handling
    return (Expr *) preprocess_expression(root, (Node *) expr, EXPRKIND_PHV);
}
```