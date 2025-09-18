# SS_process_sublinks

## Location
src/backend/optimizer/plan/subselect.c: 1919 - 1928

## Overview
The main entry point function that transforms SubLink nodes into SubPlan nodes within a given expression tree during query planning.

## Definition
```c
Node *SS_process_sublinks(PlannerInfo *root, Node *expr, bool isQual)
```

## Detailed Description
SS_process_sublinks serves as the primary interface for sublink processing in PostgreSQL query planning. It initiates the transformation of SubLink nodes (which represent subqueries in the parse tree) into SubPlan nodes (which represent the execution plans for those subqueries). The function sets up the necessary context and delegates the actual tree walking and transformation to process_sublinks_mutator.

This function is a critical component of the query planning phase, as it handles one of the most complex aspects of SQL query processing - subquery evaluation. The transformation from SubLinks to SubPlans involves determining how subqueries should be executed (as independent subplans, as joins, etc.) and integrating them into the overall query execution plan.

## Parameters / Member Variables
- `root`: PlannerInfo structure containing global planning state and context information
- `expr`: The expression tree (Node) that may contain SubLink nodes to be processed
- `isQual`: Boolean flag indicating whether the expression is a WHERE/HAVING qualifier, which affects how NULL/FALSE distinctions are handled

## Dependencies
- Functions called/Symbols referenced:
  - process_sublinks_context (struct)
  - process_sublinks_mutator
- Called from (representative examples):
  - preprocess_expression
  - build_subplan

## Notes and Other Information
- The isQual parameter is important for optimization: when processing top-level WHERE/HAVING qualifiers, the distinction between FALSE and UNKNOWN (NULL) return values from sublinks can be ignored, potentially enabling more efficient execution strategies.
- This function is part of the broader subselect processing framework in PostgreSQL optimizer.
- The actual work is delegated to process_sublinks_mutator, which performs the recursive tree traversal and node transformation.