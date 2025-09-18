# is_simple_subquery

## Location
src/backend/optimizer/prep/prepjointree.c: 1659 - 1798

## Overview
Determines whether a subquery is simple enough to be pulled up (flattened) into its parent query, checking for various conditions that would prevent safe subquery elimination.

## Definition


## Detailed Description
This function performs a comprehensive analysis to determine if a subquery can be safely pulled up into its parent query as part of PostgreSQL's query optimization process. Subquery pullup is a critical optimization that eliminates unnecessary subquery evaluation by flattening the query structure, but it must only be performed when semantically safe.

The function checks multiple conditions that would prevent pullup:

1. **Basic validity**: Ensures the subquery is a valid SELECT statement
2. **Set operations**: Rejects subqueries with set operations (UNION, INTERSECT, EXCEPT) unless handled by specialized code paths
3. **Complex operations**: Prevents pullup of subqueries containing:
   - Aggregation functions or window functions
   - Set-returning functions in target list
   - GROUP BY, HAVING, ORDER BY, DISTINCT clauses
   - LIMIT/OFFSET clauses
   - WITH (Common Table Expressions)
   - Explicit FOR UPDATE/SHARE locks
4. **Security barriers**: Blocks pullup of security-barrier views to prevent information leakage
5. **LATERAL references**: Performs complex analysis of lateral references to ensure pullup won't violate scoping rules or create qual-postponement issues
6. **Volatile functions**: Prevents pullup when the target list contains volatile functions to avoid multiple evaluations

The LATERAL reference checking is particularly sophisticated, ensuring that lateral references don't cross outer join boundaries in ways that would change query semantics.

## Parameters / Member Variables
- : PlannerInfo containing the overall query planning context and relation information
- : The Query node representing the subquery being evaluated for pullup (may be a processed copy, not necessarily equal to rte->subquery)
- : The RangeTblEntry of type RTE_SUBQUERY that contains the subquery being analyzed
- : The lowest outer join expression above the subquery in the query tree, or NULL if no outer joins are present

## Dependencies
- Functions called/Symbols referenced:
  - IsA (type checking macro)
  - elog (error logging)
  - get_relids_in_jointree
  - jointree_contains_lateral_outer_refs
  - pull_varnos_of_level
  - bms_is_subset
  - contain_volatile_functions
- Called from (representative examples):
  - pull_up_subqueries_recurse
  - pull_up_simple_subquery

## Notes and Other Information
- The function is static, limiting its scope to the prepjointree.c compilation unit
- Returns true only if the subquery can be safely pulled up, false otherwise
- The function is conservative - it errs on the side of safety rather than risking semantic changes
- Special handling for implicit vs explicit FOR UPDATE/SHARE clauses: implicit locking (inherited from parent) doesn't prevent pullup, but explicit locking does
- The LATERAL reference analysis includes sophisticated logic to handle outer join interactions and prevent qual-postponement issues
- Future PostgreSQL versions might relax some restrictions (like WITH clauses) as indicated by XXX comments in the code
- The volatile function check prevents subtle bugs that could arise from multiple function evaluations after pullup