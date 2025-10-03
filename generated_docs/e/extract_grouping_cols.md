# extract_grouping_cols

## Location
[src/backend/optimizer/util/tlist.c:514-539](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/tlist.c#L514-L539)

## Overview
Extracts result column numbers (resnos) from grouping columns in a SortGroupClause list and returns them as an array for use in planning GROUP BY operations.

## Definition

```c
AttrNumber *
extract_grouping_cols(List *groupClause, List *tlist)
```
## Detailed Description
This utility function processes a list of SortGroupClause structures alongside a target list to extract the result column numbers (resnos) of the grouping columns. For each SortGroupClause, it locates the corresponding TargetEntry in the target list using get_sortgroupclause_tle(), then extracts the resno field from that TargetEntry. The function creates and returns a dynamically allocated array containing these column numbers in the same order as they appear in the input list.

The result column numbers (resnos) are essential for execution plan nodes to identify which columns from their input tuples should be used for grouping. This information allows the execution engine to efficiently access the correct columns during the grouping process without needing to re-parse expressions.

## Parameters / Member Variables
- `*groupClause`: A List of SortGroupClause structures representing the grouping columns
- `*tlist`: A List of TargetEntry structures containing the target expressions to extract column numbers from
## Dependencies
- Functions called/Symbols referenced:
  - [list_length](../l/list_length.md) (to determine array size)
  - [palloc](../p/palloc.md) (for memory allocation)
  - lfirst (for list iteration)
  - [get_sortgroupclause_tle](../g/get_sortgroupclause_tle.md) (to find corresponding TargetEntry)
  - [SortGroupClause](../S/SortGroupClause.md) (structure type)
  - [TargetEntry](../T/TargetEntry.md) (structure type)
  - AttrNumber (type for column numbers)
- Called from (representative examples):
  - [create_group_plan](../c/create_group_plan.md) (src/backend/optimizer/plan/createplan.c:2262)
  - [create_agg_plan](../c/create_agg_plan.md) (src/backend/optimizer/plan/createplan.c:2330)

## Notes and Other Information
- The returned array is allocated with palloc() and becomes the caller's responsibility to manage
- Returns AttrNumber values which are typically 1-based column indices
- Works in conjunction with extract_grouping_ops and extract_grouping_collations to provide complete grouping information
- Essential for execution plan nodes to efficiently identify grouping columns in input tuples
- Located in src/backend/optimizer/util/tlist.c:514-539

## Simplified Source

```c
AttrNumber *
extract_grouping_cols(List *groupClause, List *tlist)
{
    int numCols = list_length(groupClause);
    AttrNumber *grpColIdx = (AttrNumber *) palloc(sizeof(AttrNumber) * numCols);
    int colno = 0;

    // Extract column number from each grouping clause
    foreach(glitem, groupClause)
    {
        SortGroupClause *groupcl = (SortGroupClause *) lfirst(glitem);
        TargetEntry *tle = get_sortgroupclause_tle(groupcl, tlist);

        // Store the result column number
        grpColIdx[colno++] = tle->resno;
    }

    return grpColIdx;
}
```