# find_var_for_subquery_tle

## Location
src/backend/optimizer/path/pathkeys.c: 1249 - 1291

## Overview
Determines if a subquery target list entry is visible to the outer query and returns the corresponding Var reference if available.

## Definition


## Detailed Description
This function serves as a bridge between subquery internal representations and outer query visibility. It checks whether a specific target list entry from a subquery will be accessible to the outer query by searching the relation's target expressions.

The function performs several validation steps:
1. Immediately returns NULL for resjunk entries, as these are internal-only and not visible to outer queries
2. Searches through the relation's target list expressions to find a matching Var
3. Validates that found variables reference the correct relation and attribute number
4. Returns a copy of the matching Var to ensure memory safety

This is crucial for pathkey conversion, as it prevents the creation of pathkeys that reference values not actually available above the subquery level.

## Parameters / Member Variables
- : RelOptInfo representing the subquery relation in the outer query context
- : TargetEntry from the subquery's target list to check for visibility

## Dependencies
- Functions called/Symbols referenced:
  - copyObject (to create a safe copy of the Var node)
- Called from (representative examples):
  - convert_subquery_pathkeys (multiple calls for pathkey conversion)

## Notes and Other Information
- This is a static helper function used internally within the pathkeys module
- Essential for ensuring pathkey validity across subquery boundaries
- Resjunk entries are automatically excluded as they're not visible to outer queries
- Returns a copied Var node to prevent memory management issues
- Part of PostgreSQL's subquery optimization system for maintaining proper variable references