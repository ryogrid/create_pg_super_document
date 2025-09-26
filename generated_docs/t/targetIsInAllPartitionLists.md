# targetIsInAllPartitionLists

## Location
[src/backend/optimizer/path/allpaths.c:3812-3854](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/allpaths.c#L3812-L3854)

## Overview
This function determines whether a given TargetEntry appears in the PARTITION BY clause of every window definition in a query, which is important for qualifier pushdown safety analysis.

## Definition
```c
static bool targetIsInAllPartitionLists(TargetEntry *tle, Query *query)
```

## Detailed Description
This function is used as part of the qualifier pushdown optimization process to determine if it's safe to push down qualifiers that reference specific columns. When a TargetEntry (column) appears in the PARTITION BY clause of all window functions in a query, it means the column is used for partitioning data across all windows, making it potentially safer for certain optimization decisions.

The function iterates through all WindowClause structures in the query's windowClause list and checks if the given TargetEntry is present in each window's partitionClause using the targetIsInSortList helper function. If any window lacks this target in its partition clause, the function returns false.

The implementation notes that it would be possible to optimize by ignoring windows not actually used by any window function, but this optimization is not implemented due to the complexity of determining unused windows at this stage and the rarity of unreferenced window definitions in practice.

## Parameters / Member Variables
- `tle`: The TargetEntry to check for presence in partition clauses
- `query`: The Query structure containing the window clauses to examine

## Dependencies
- Functions called/Symbols referenced:
  - lfirst
  - [targetIsInSortList](targetIsInSortList.md)
- Types referenced:
  - [TargetEntry](../T/TargetEntry.md)
  - [Query](../Q/Query.md)
  - [WindowClause](../W/WindowClause.md)
  - InvalidOid (constant)
- Called from (representative examples):
  - [check_output_expressions](../c/check_output_expressions.md) (src/backend/optimizer/path/allpaths.c:3752)

## Notes and Other Information
- This is a static function within allpaths.c, used specifically for window function-related pushdown safety analysis
- The function returns true if there are no window clauses in the query (vacuously true)
- Part of the broader qualifier pushdown safety framework in PostgreSQL's optimizer
- Performance consideration: Does not optimize for unused window definitions to keep implementation simple
- Located in src/backend/optimizer/path/allpaths.c:3812-3854