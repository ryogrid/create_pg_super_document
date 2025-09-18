# set_function_pathlist

## Location
src/backend/optimizer/path/allpaths.c: 2749 - 2815

## Overview
Builds the single access path for a function RTE (Range Table Entry), handling the pathlist generation for function scans in PostgreSQL's query planner.

## Definition


## Detailed Description
This function is responsible for creating access paths for function RTEs in PostgreSQL's query optimizer. It handles the specific case where a function is used as a data source in a query (e.g., SELECT * FROM my_function()). The function determines the required outer relations for parameterization due to LATERAL references and optionally creates pathkeys for ordering when the ORDINALITY clause is used.

When ORDINALITY is specified in a function call, PostgreSQL adds an ordinal column that numbers the rows returned by the function. This function detects this case and attempts to build pathkeys for the ordinality column if it's referenced in equivalence classes, allowing the optimizer to take advantage of the inherent ordering.

## Parameters / Member Variables
- : PlannerInfo structure containing global information about the query being planned
- : RelOptInfo structure representing the relation (function) for which paths are being generated
- : RangeTblEntry representing the function in the query's range table

## Dependencies
- Functions called/Symbols referenced:
  - build_expression_pathkey
  - add_path
  - create_functionscan_path
- Called from (representative examples):
  - set_rel_pathlist

## Notes and Other Information
- Function scans do not support pushing join clauses into their quals, but they can still have required parameterization due to LATERAL references in the function expression
- The function result is considered unordered unless ORDINALITY was used, in which case it's ordered by the ordinal column (the last column)
- The function checks if the ordinality column is actually referenced in the query's targetlist before attempting to build pathkeys for it
- Uses Int8LessOperator for sorting the ordinality column since ordinal numbers are stored as int8 (bigint) type
- Located in src/backend/optimizer/path/allpaths.c:2749-2815