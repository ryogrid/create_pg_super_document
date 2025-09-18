# compare_tlist_datatypes

## Location
src/backend/optimizer/path/allpaths.c: 3779 - 3811

## Overview
This function compares the data types in a subquery's target list against the expected output types from a set operation (UNION/INTERSECT), marking columns as unsafe for pushdown when type mismatches are detected.

## Definition


## Detailed Description
The function is used in the context of qualifier pushdown optimization for set operations (UNION/UNION ALL/INTERSECT/INTERSECT ALL). When PostgreSQL attempts to push WHERE clause conditions into component queries of a set operation, it must ensure that no semantic issues arise from type coercions.

The function iterates through each non-junk entry in the target list and compares its expression type with the corresponding expected output type from the set operation. If a type mismatch is found, it sets the UNSAFE_TYPE_MISMATCH flag in the unsafeFlags array for that column, preventing qualifier pushdown for that specific column.

The function assumes that typmod differences are acceptable as long as the base types match, since the only allowed typmod difference is from a specific typmod to -1 (generic), which doesn't require coercion.

## Parameters / Member Variables
- : The target list of a component subquery in the set operation
- : List of OID values representing the output column types of the top-level set operation
- : Pointer to pushdown_safety_info structure where unsafeFlags array will be updated

## Dependencies
- Functions called/Symbols referenced:
  - list_head
  - lfirst
  - lfirst_oid
  - [lnext](../l/lnext.md)
  - exprType
  - elog
- Types referenced:
  - [pushdown_safety_info](../p/pushdown_safety_info.md)
  - [TargetEntry](../T/TargetEntry.md)
  - UNSAFE_TYPE_MISMATCH (constant)
- Called from (representative examples):
  - [subquery_is_pushdown_safe](../s/subquery_is_pushdown_safe.md) (src/backend/optimizer/path/allpaths.c:3627)

## Notes and Other Information
- This is a static function within allpaths.c, used specifically for set operation qualifier pushdown safety analysis
- The function performs strict type checking - any type mismatch results in marking the column as unsafe
- Resjunk columns are ignored as they don't participate in the final result set
- Error checking ensures the target list length matches the expected column types list length
- Located in src/backend/optimizer/path/allpaths.c:3779-3811