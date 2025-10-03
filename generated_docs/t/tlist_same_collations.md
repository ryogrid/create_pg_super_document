# tlist_same_collations

## Location
[src/backend/optimizer/util/tlist.c:282-317](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/tlist.c#L282-L317)

## Overview
Compares a target list's exposed collations against a specified list of expected collations, with optional handling of junk columns.

## Definition

```c
bool
tlist_same_collations(List *tlist, List *colCollations, bool junkOK)
```
## Detailed Description
This function verifies whether a target list produces the same collations as specified in a given list of expected collations. It follows identical logic to tlist_same_datatypes but focuses on collation compatibility rather than datatype compatibility. This is crucial for ensuring correct string comparison and sorting behavior in set operations and other query planning scenarios.

Collations determine how text data is compared and sorted, including case sensitivity, accent sensitivity, and locale-specific rules. The function ensures that different parts of a query plan will handle text data consistently according to the expected collation rules.

Like its datatype counterpart, this function handles resjunk columns based on the junkOK parameter, allowing callers to either ignore or reject the presence of auxiliary columns.

## Parameters / Member Variables
- `*tlist`: The target list whose collations are to be checked
- `*colCollations`: List of Oid values representing the expected collations
- `junkOK`: Whether to ignore resjunk columns (true) or reject them (false)
## Dependencies
- Functions called/Symbols referenced:
  - [list_head](../l/list_head.md) (to get the first element of colCollations list)
  - [exprCollation](../e/exprCollation.md) (to get the collation of an expression)
  - [lnext](../l/lnext.md) (to advance through colCollations list)
  - lfirst_oid (to extract Oid from list cell)
  - [TargetEntry](../T/TargetEntry.md) (struct type)
- Called from (representative examples):
  - [recurse_set_operations](../r/recurse_set_operations.md)

## Notes and Other Information
- Returns false if the target list is longer or shorter than the expected collations list
- Only non-junk columns are counted when junkOK is true
- Used primarily in set operations (UNION, INTERSECT, EXCEPT) to ensure collation compatibility
- Collation compatibility is essential for correct text handling in international applications
- The function assumes colCollations contains Oid values representing PostgreSQL collation OIDs
- Works in conjunction with tlist_same_datatypes to ensure complete type and collation compatibility

## Simplified Source

```c
bool
tlist_same_collations(List *tlist, List *colCollations, bool junkOK)
{
    ListCell   *l;
    ListCell   *curColColl = list_head(colCollations);

    foreach(l, tlist)
    {
        TargetEntry *tle = (TargetEntry *) lfirst(l);

        if (tle->resjunk)
        {
            // Reject junk columns if not allowed
            if (!junkOK)
                return false;
        }
        else
        {
            // Check if we have more non-junk columns than expected collations
            if (curColColl == NULL)
                return false;

            // Compare actual collation with expected collation
            if (exprCollation((Node *) tle->expr) != lfirst_oid(curColColl))
                return false;

            curColColl = lnext(colCollations, curColColl);
        }
    }

    // Check if we have fewer non-junk columns than expected collations
    if (curColColl != NULL)
        return false;

    return true;
}
```