# extract_grouping_collations

## Location
[src/backend/optimizer/util/tlist.c:489-513](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/tlist.c#L489-L513)

## Overview
Extracts collation OIDs from grouping column expressions in a SortGroupClause list and returns them as an array for use in planning GROUP BY operations.

## Definition

```c
Oid *
extract_grouping_collations(List *groupClause, List *tlist)
```
## Detailed Description
This utility function processes a list of SortGroupClause structures alongside a target list to extract the collation OIDs from the expressions of grouping columns. For each SortGroupClause, it locates the corresponding TargetEntry in the target list, then extracts the collation OID from that entry's expression using exprCollation(). The function creates and returns a dynamically allocated array containing these collation OIDs in the same order as they appear in the input list.

This function is essential for query planning as it provides the collation information needed for proper comparison and sorting of grouped data. The collation determines how text data should be compared and ordered, which is crucial for correct grouping behavior in multi-lingual or case-sensitive scenarios.

## Parameters / Member Variables
- : A List of SortGroupClause structures representing the grouping columns
- : A List of TargetEntry structures containing the target expressions to extract collations from

## Dependencies
- Functions called/Symbols referenced:
  - [list_length](../l/list_length.md) (to determine array size)
  - [palloc](../p/palloc.md) (for memory allocation)
  - lfirst (for list iteration)
  - [get_sortgroupclause_tle](../g/get_sortgroupclause_tle.md) (to find corresponding TargetEntry)
  - [exprCollation](exprCollation.md) (to extract collation from expression)
  - [SortGroupClause](../S/SortGroupClause.md) (structure type)
  - [TargetEntry](../T/TargetEntry.md) (structure type)
- Called from (representative examples):
  - [create_group_plan](../c/create_group_plan.md) (src/backend/optimizer/plan/createplan.c:2265)
  - [create_agg_plan](../c/create_agg_plan.md) (src/backend/optimizer/plan/createplan.c:2333)
  - [create_groupingsets_plan](../c/create_groupingsets_plan.md) (src/backend/optimizer/plan/createplan.c:2491, 2530)

## Notes and Other Information
- The returned array is allocated with palloc() and becomes the caller's responsibility to manage
- Requires both the SortGroupClause list and the target list to properly resolve expressions and their collations
- Works in conjunction with extract_grouping_ops and extract_grouping_cols to provide complete grouping information
- Essential for proper handling of collation-sensitive grouping operations
- Located in src/backend/optimizer/util/tlist.c:489-513

## Simplified Source

```c
Oid *
extract_grouping_collations(List *groupClause, List *tlist)
{
    int numCols = list_length(groupClause);
    Oid *grpCollations = (Oid *) palloc(sizeof(Oid) * numCols);
    int colno = 0;

    // Extract collation from each grouping column
    foreach(glitem, groupClause)
    {
        SortGroupClause *groupcl = (SortGroupClause *) lfirst(glitem);
        TargetEntry *tle = get_sortgroupclause_tle(groupcl, tlist);

        // Get collation from the target entry's expression
        grpCollations[colno++] = exprCollation((Node *) tle->expr);
    }

    return grpCollations;
}
```