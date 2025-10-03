# tlist_same_datatypes

## Location
[src/backend/optimizer/util/tlist.c:248-281](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/tlist.c#L248-L281)

## Overview
Compares a target list's output datatypes against a specified list of column types, with optional handling of junk columns.

## Definition

```c
bool
tlist_same_datatypes(List *tlist, List *colTypes, bool junkOK)
```
## Detailed Description
This function verifies whether a target list produces the same output datatypes as specified in a given list of column types. It's primarily used during query planning to ensure type compatibility between different parts of a query plan, particularly in set operations like UNION.

The function handles resjunk columns (auxiliary columns used internally by the planner but not part of the final result) based on the junkOK parameter. When junkOK is false, any presence of junk columns will cause the function to return false. When junkOK is true, junk columns are simply ignored during the comparison.

Note that the function currently only compares base datatypes and does not consider type modifiers (typmods), as no current callers require that level of precision.

## Parameters / Member Variables
- `*tlist`: The target list whose datatypes are to be checked
- `*colTypes`: List of Oid values representing the expected column types
- `junkOK`: Whether to ignore resjunk columns (true) or reject them (false)
## Dependencies
- Functions called/Symbols referenced:
  - [list_head](../l/list_head.md) (to get the first element of colTypes list)
  - [lnext](../l/lnext.md) (to advance through colTypes list)
  - [exprType](../e/exprType.md) (to get the datatype of an expression)
  - lfirst_oid (to extract Oid from list cell)
  - [TargetEntry](../T/TargetEntry.md) (struct type)
- Called from (representative examples):
  - [is_simple_union_all_recurse](../i/is_simple_union_all_recurse.md)
  - [recurse_set_operations](../r/recurse_set_operations.md)

## Notes and Other Information
- Returns false if the target list is longer or shorter than the expected column types list
- Only non-junk columns are counted when junkOK is true
- Used primarily in set operations (UNION, INTERSECT, EXCEPT) to ensure type compatibility
- Type modifiers (precision, scale, etc.) are not compared, only base types
- The function assumes colTypes contains Oid values representing PostgreSQL type OIDs

## Simplified Source

```c
bool tlist_same_datatypes(List *tlist, List *colTypes, bool junkOK) {
    ListCell *curColType = list_head(colTypes);

    // Compare each target entry with expected column types
    foreach(l, tlist) {
        TargetEntry *tle = (TargetEntry *) lfirst(l);

        if (tle->resjunk) {
            // Skip or reject junk columns based on junkOK flag
            if (!junkOK)
                return false;
        } else {
            // Check if we have a corresponding expected type
            if (curColType == NULL)
                return false;  // Target list is longer than expected

            // Compare the actual type with expected type
            if (exprType((Node *) tle->expr) != lfirst_oid(curColType))
                return false;

            // Move to next expected type
            curColType = lnext(colTypes, curColType);
        }
    }

    // Ensure we've consumed all expected types
    if (curColType != NULL)
        return false;  // Target list is shorter than expected

    return true;
}
```