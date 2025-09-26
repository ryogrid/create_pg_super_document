# RecursiveUnionPath

## Location
src/include/nodes/pathnodes.h: 2347 - 2355

## Overview
RecursiveUnionPath represents a path for recursive UNION operations in PostgreSQL, used to implement Common Table Expressions (CTEs) with recursive queries.

## Definition


## Detailed Description
RecursiveUnionPath represents the execution path for recursive UNION operations in PostgreSQL's query planner, primarily used for implementing recursive Common Table Expressions (CTEs). This path type handles the iterative execution pattern where the left path provides the base case (non-recursive term) and the right path provides the recursive term that references the CTE itself. The path manages the work table that stores intermediate results during recursive evaluation and handles duplicate elimination when required.

## Parameters / Member Variables
- : Base Path structure containing cost estimates, expected row count, and other standard path properties
- : Pointer to the path representing the non-recursive term (base case) of the recursive UNION
- : Pointer to the path representing the recursive term that references the CTE being defined
- : List of SortGroupClause structures identifying columns used for duplicate elimination (NULL for UNION ALL)
- : Parameter ID for the work table that stores intermediate results during recursive execution
- : Estimated cardinality representing the expected number of distinct groups in the combined input

## Dependencies
- Functions called/Symbols referenced:
  - Cardinality
- Called from (representative examples):
  - create_recursiveunion_plan
  - create_recursiveunion_path
  - create_plan_recurse

## Notes and Other Information
- Recursive CTEs require careful cycle detection and termination conditions to prevent infinite loops
- The work table (identified by wtParam) is used to pass results from one iteration to the next
- The distinctList is only used when UNION (not UNION ALL) is specified, requiring duplicate elimination
- PostgreSQL limits recursive CTE depth to prevent runaway queries and stack overflow