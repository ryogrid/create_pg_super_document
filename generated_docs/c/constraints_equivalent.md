# constraints_equivalent

## Location
src/backend/commands/tablecmds.c: 15867 - 15895

## Overview
constraints_equivalent is a static function that determines whether two check constraints are functionally equivalent by comparing their properties and decompiled source expressions.

## Definition


## Detailed Description
This function compares two check constraints to determine if they are functionally equivalent. The comparison is performed in two stages: first checking constraint properties (deferrable flags), then comparing the actual constraint expressions by decompiling them to source text. The function uses string comparison of the decompiled expressions rather than binary comparison to handle cases where constraints may be logically equivalent but stored differently (e.g., due to different column numbers between parent and child relations in inheritance scenarios).

The function checks:
1. Whether both constraints have the same condeferrable setting
2. Whether both constraints have the same condeferred setting  
3. Whether the decompiled constraint expressions are textually identical

If any of these comparisons fail, the constraints are considered non-equivalent.

## Parameters / Member Variables
- : HeapTuple containing the first pg_constraint row to compare
- : HeapTuple containing the second pg_constraint row to compare
- : TupleDesc describing the structure of the pg_constraint tuples

## Dependencies
- Functions called/Symbols referenced:
  - GETSTRUCT (macro)
  - decompile_conbin (called twice)
  - strcmp
  - Form_pg_constraint
- Called from (representative examples):
  - MergeConstraintsIntoExisting

## Notes and Other Information
- The function assumes both input tuples are from pg_constraint catalog
- Uses text-based comparison of decompiled expressions to handle column number differences between parent/child relations
- The decompile_conbin function handles the complexity of converting binary constraint expressions to comparable text form
- This function is primarily used during inheritance and partitioning operations to determine if constraints need to be merged or are already equivalent
- Returns true only if constraints are identical in all checked aspects (deferrable flags and expression text)