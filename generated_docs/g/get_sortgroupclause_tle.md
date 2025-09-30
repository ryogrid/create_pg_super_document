# get_sortgroupclause_tle

## Location
[src/backend/optimizer/util/tlist.c:367-378](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/tlist.c#L367-L378)

## Overview
Finds the targetlist entry matching the given SortGroupClause by ressortgroupref and returns it.

## Definition
TargetEntry *get_sortgroupclause_tle(SortGroupClause *sgClause, List *targetList)

## Detailed Description
This function serves as a convenience wrapper around get_sortgroupref_tle(). It extracts the tleSortGroupRef field from a SortGroupClause structure and uses it to locate the corresponding TargetEntry in the provided target list. This is commonly needed during query planning when the system needs to find the actual expression associated with a sort or grouping clause.

## Parameters / Member Variables
- : Pointer to a SortGroupClause structure containing the sort/group reference information
- : List of TargetEntry nodes to search within

## Dependencies
- Functions called/Symbols referenced:
  - [get_sortgroupref_tle](get_sortgroupref_tle.md)
  - [SortGroupClause](../S/SortGroupClause.md) (structure type)
- Called from (representative examples):
  - [build_pertrans_for_aggref](../b/build_pertrans_for_aggref.md)
  - [query_is_distinct_for](../q/query_is_distinct_for.md)
  - [create_groupingsets_plan](../c/create_groupingsets_plan.md)
  - [create_windowagg_plan](../c/create_windowagg_plan.md)
  - [make_sort_from_sortclauses](../m/make_sort_from_sortclauses.md)
  - [get_sortgroupclause_expr](get_sortgroupclause_expr.md)

## Notes and Other Information
This function is a simple wrapper that provides a more convenient interface when working with SortGroupClause structures. It's widely used throughout the PostgreSQL optimizer and executor for operations involving sorting and grouping.

## Simplified Source

```c
TargetEntry *get_sortgroupclause_tle(SortGroupClause *sgClause, List *targetList) {
    return get_sortgroupref_tle(sgClause->tleSortGroupRef, targetList);
}
```