# get_sortgroupref_tle

## Location
[src/backend/optimizer/util/tlist.c:345-366](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/tlist.c#L345-L366)

## Overview
Finds and returns the target list entry that matches a given SortGroupRef index.

## Definition

```c
TargetEntry *
get_sortgroupref_tle(Index sortref, List *targetList)
```
## Detailed Description
This function searches through a target list to find the TargetEntry that has a matching ressortgroupref value. The ressortgroupref field is used to link target list entries with ORDER BY, GROUP BY, and DISTINCT clauses. When the parser processes these clauses, it assigns unique reference numbers to expressions that need to be sorted or grouped, and this function provides a way to locate the corresponding target list entry.

The function performs a linear search through the target list and returns the first matching entry. If no matching entry is found, it raises an ERROR with the message "ORDER/GROUP BY expression not found in targetlist", indicating a serious internal inconsistency in the query planning process.

This is a utility function that's essential for connecting the abstract sorting/grouping requirements with the actual expressions in the target list.

## Parameters / Member Variables
- `sortref`: The SortGroupRef index value to search for
- `*targetList`: The target list to search through
## Dependencies
- Functions called/Symbols referenced:
  - foreach (macro for list iteration)
  - lfirst (to extract list cell content)
  - elog (for error reporting)
  - [TargetEntry](../T/TargetEntry.md) (struct type)
  - Index (type alias)
- Called from (representative examples):
  - [convert_subquery_pathkeys](../c/convert_subquery_pathkeys.md)
  - [prepare_sort_from_pathkeys](../p/prepare_sort_from_pathkeys.md)
  - [make_unique_from_pathkeys](../m/make_unique_from_pathkeys.md)
  - [get_sortgroupclause_tle](get_sortgroupclause_tle.md)
  - [transformDistinctOnClause](../t/transformDistinctOnClause.md)
  - [get_rule_sortgroupclause](get_rule_sortgroupclause.md)

## Notes and Other Information
- Throws an ERROR if the sortref is not found, which should never happen in correct code
- Returns NULL after the error (unreachable code, kept for compiler satisfaction)
- Used extensively in sorting and grouping operations throughout the optimizer
- The SortGroupRef mechanism allows the same expression to be referenced from multiple clauses
- Linear search is acceptable since target lists are typically small
- Essential for implementing ORDER BY, GROUP BY, DISTINCT, and window function operations

## Simplified Source

```c
TargetEntry *get_sortgroupref_tle(Index sortref, List *targetList)
{
    // Search through target list for matching SortGroupRef
    foreach(l, targetList) {
        TargetEntry *tle = (TargetEntry *) lfirst(l);

        if (tle->ressortgroupref == sortref)
            return tle;
    }

    // Should never happen in correct code
    elog(ERROR, "ORDER/GROUP BY expression not found in targetlist");
    return NULL;  // Keep compiler quiet
}
```