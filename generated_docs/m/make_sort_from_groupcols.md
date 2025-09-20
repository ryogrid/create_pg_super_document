# make_sort_from_groupcols

## Location
[src/backend/optimizer/plan/createplan.c:6465-6505](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/createplan.c#L6465-L6505)

## Overview
Creates a Sort plan node to sort input tuples based on grouping columns, using pre-specified column indices rather than extracting them from sort group clauses.

## Definition

```c
static Sort *
make_sort_from_groupcols(List *groupcls,
						 AttrNumber *grpColIdx,
						 Plan *lefttree)
```
## Detailed Description
This function constructs a Sort plan node specifically for grouping operations where the target columns are already identified by their column indices. Unlike make_sort_from_sortclauses, this function must use the provided grpColIdx array to locate sort columns because the child plan's target list is not marked with ressortgroupref info appropriate to the grouping node. It extracts only the sort ordering information from the SortGroupClause entries, relying on the grpColIdx array for column identification.

The function performs similar array allocation and population as make_sort_from_sortclauses but uses get_tle_by_resno() instead of get_sortgroupclause_tle() to retrieve target entries, and includes error checking for missing target entries.

## Parameters / Member Variables
- : A List of SortGroupClause structures specifying the grouping/sort criteria
- : An array of AttrNumber values specifying which columns to sort by
- : The input Plan node that provides the tuples to be sorted

## Dependencies
- Functions called/Symbols referenced:
  - SortGroupClause (struct type)
  - [get_tle_by_resno](../g/get_tle_by_resno.md)
  - [exprCollation](../e/exprCollation.md)
  - [make_sort](make_sort.md)
  - Material (related type)
- Called from (representative examples):
  - [create_groupingsets_plan](../c/create_groupingsets_plan.md)

## Notes and Other Information
- This is a static function, meaning it's only accessible within the same source file
- The key difference from make_sort_from_sortclauses is the use of grpColIdx[] to locate columns rather than extracting from SortGroupClause ressortgroupref
- Includes error handling with elog(ERROR) if a target entry cannot be retrieved
- The comment explains why this cannot be merged with make_sort_from_sortclauses despite their similarity
- Located in src/backend/optimizer/plan/createplan.c at lines 6465-6505