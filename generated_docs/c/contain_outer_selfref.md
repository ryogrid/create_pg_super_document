# contain_outer_selfref

## Location
src/backend/optimizer/plan/subselect.c: 1083 - 1096

## Overview
Determines whether there is an external recursive self-reference within a query tree by checking for CTE (Common Table Expression) self-references that reference CTEs defined at a higher query level than the current context.

## Definition


## Detailed Description
This function serves as the entry point for detecting external recursive self-references in PostgreSQL query trees. It specifically looks for cases where a CTE (Common Table Expression) references itself, but the reference occurs at a query nesting level that is outside (higher than) the CTE's own definition level. This is important for query optimization decisions, particularly when determining whether CTEs can be inlined or need special handling for recursive operations.

The function acts as a wrapper that initializes the depth tracking to 0 and delegates the actual traversal work to . The depth parameter is crucial because it tracks the current nesting level of queries as the walker traverses the query tree, allowing the system to determine whether a self-reference crosses query boundaries.

## Parameters / Member Variables
- : The root Node of the query tree to examine (expected to be a Query node)

## Dependencies
- Functions called/Symbols referenced:
  - [contain_outer_selfref_walker](contain_outer_selfref_walker.md)
  - IsA (macro for type checking)
  - Assert (macro for debugging assertions)
- Called from (representative examples):
  - [SS_process_ctes](../S/SS_process_ctes.md)

## Notes and Other Information
- The function expects the input node to be a Query node, as indicated by the Assert statement
- This function is part of the subquery processing logic in PostgreSQL's query optimizer
- The depth tracking starts at 0, meaning depth will be 1 when examining the immediate contents of the input Query
- External recursive self-references can affect whether a CTE can be inlined during optimization
- This is a static function, meaning it's only accessible within the subselect.c compilation unit