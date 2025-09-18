# plan_set_operations

## Location
src/backend/optimizer/prep/prepunion.c: 99 - 187

## Overview
The main entry point for planning set operations (UNION/INTERSECT/EXCEPT) in PostgreSQL's query optimizer, handling the overall coordination of set operation tree processing.

## Definition


## Detailed Description
 is responsible for planning and optimizing a tree of set operations in PostgreSQL. This function serves as the main coordinator for handling UNION, INTERSECT, and EXCEPT operations. It takes a parsed query containing set operations and produces optimized execution paths.

The function performs several key tasks:
- Validates that the query structure is suitable for set operations (no joins, GROUP BY, HAVING, etc.)
- Sets up the equivalence class merging state for the optimizer
- Prepares relation arrays for subqueries involved in the set operations
- Identifies the leftmost query component to determine column naming conventions
- Delegates to specialized functions based on whether the operation involves recursion
- Returns a RelOptInfo containing optimized paths for executing the set operation tree

For recursive operations (Common Table Expressions with UNION), it calls . For non-recursive operations, it calls  to build the execution plan recursively through the set operation tree.

## Parameters / Member Variables
- : PlannerInfo structure containing the query context, parse tree, and optimizer state information

## Dependencies
- Functions called/Symbols referenced:
  - castNode
  - setup_simple_rel_arrays
  - generate_recursion_path
  - recurse_set_operations
  - SetOperationStmt
  - RangeTblRef
- Called from (representative examples):
  - grouping_planner (src/backend/optimizer/plan/planner.c:1377)

## Notes and Other Information
- This function only handles the setOperations tree itself; top-level ORDER BY and LIMIT clauses are handled by the calling function (grouping_planner)
- The function sets  to indicate that equivalence class merging is complete, allowing pathkey generation
- Column names for all generated target lists are taken from the leftmost component query to ensure compatibility with SELECT INTO statements
- The function validates several constraints through assertions, ensuring the query doesn't contain unsupported combinations with set operations
- The resulting RelOptInfo is an "upperrel" that represents the output of the entire set operation tree