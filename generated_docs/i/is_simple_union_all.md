# is_simple_union_all

## Location
src/backend/optimizer/prep/prepjointree.c: 2072 - 2099

## Overview
This function checks if a subquery is a simple UNION ALL operation that meets specific criteria for optimization eligibility.

## Definition
```c
static bool is_simple_union_all(Query *subquery)
```

## Detailed Description
The is_simple_union_all function determines whether a subquery can be optimized as a simple UNION ALL operation. This is a key step in PostgreSQL's query optimization process that enables certain subqueries to be flattened or otherwise optimized.

The function performs several validation checks:
1. Verifies the input is a valid SELECT query
2. Confirms it contains set operations (UNION, INTERSECT, EXCEPT)
3. Ensures there are no complicating clauses like ORDER BY, LIMIT, OFFSET, row locking, or CTEs
4. Recursively validates that all set operations in the tree are UNION ALL with compatible datatypes

The function is conservative in its approach - it only approves subqueries that are purely UNION ALL operations without any mixing of different set operation types and without datatype coercions between the leaf queries.

## Parameters / Member Variables
- `subquery`: Query structure representing the subquery to be analyzed for UNION ALL simplicity

## Dependencies
- Functions called/Symbols referenced:
  - SetOperationStmt
  - CMD_SELECT  
  - is_simple_union_all_recurse
- Called from:
  - pull_up_subqueries_recurse

## Notes and Other Information
- All setops must be UNION ALL (no mixing with INTERSECT or EXCEPT)
- No datatype coercions are allowed - all leaf queries must emit the same datatypes
- Rejects subqueries with ORDER BY, LIMIT/OFFSET, locking clauses, or CTEs
- The actual recursive validation of the set operation tree is delegated to is_simple_union_all_recurse
- This function is part of the subquery pullup optimization infrastructure
- Located in src/backend/optimizer/prep/prepjointree.c:2072-2099