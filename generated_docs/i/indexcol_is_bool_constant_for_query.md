# indexcol_is_bool_constant_for_query

## Location
src/backend/optimizer/path/indxpath.c: 3614 - 3664

## Overview
Determines if an index column is constrained to have a constant value by boolean restriction clauses in the query's WHERE conditions.

## Definition


## Detailed Description
This function addresses a specific optimization scenario for boolean index columns. When a boolean column is constrained by WHERE conditions like "WHERE boolcol" or "WHERE NOT boolcol", expression preprocessing simplifies these to boolean expressions rather than explicit equality comparisons like "WHERE boolcol = true". This means no EquivalenceClass is created for the constant value, which would normally signal that the column is irrelevant for sort-order considerations.

The function specifically handles this case by checking if a boolean index column matches any boolean restriction clauses, allowing such columns to be recognized as effectively constant for query optimization purposes, just as non-boolean columns with explicit "col = constant" restrictions are handled.

## Parameters / Member Variables
- : PlannerInfo structure containing global query information
- : IndexOptInfo structure representing the index being analyzed
- : Column number within the index to check for boolean constant constraints

## Dependencies
- Functions called/Symbols referenced:
  - IsBooleanOpfamily
  - match_boolean_index_clause
  - IndexOptInfo (structure)
  - RestrictInfo (structure)
- Called from (representative examples):
  - build_index_pathkeys

## Notes and Other Information
- Only applicable to boolean opfamily index columns (checked via IsBooleanOpfamily)
- Designed to complement the standard EquivalenceClass-based constant detection for non-boolean columns
- Skips pseudoconstant restriction clauses to avoid wasting cycles on negligible match possibilities
- Uses match_boolean_index_clause to perform the actual clause-to-column matching
- Returns true if any boolean restriction clause constrains the specified index column
- Helps ensure boolean index columns receive the same optimization treatment as other data types
- File location: src/backend/optimizer/path/indxpath.c:3614-3664