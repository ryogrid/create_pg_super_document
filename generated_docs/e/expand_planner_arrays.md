# expand_planner_arrays

## Location
[src/backend/optimizer/util/relnode.c:163-191](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/relnode.c#L163-L191)

## Overview
Dynamically expands the PlannerInfo's per-RTE arrays when additional entries are needed during query planning, particularly for inheritance and partitioning scenarios.

## Definition
```c
void expand_planner_arrays(PlannerInfo *root, int add_size)
```

## Detailed Description
This function expands the three main arrays in PlannerInfo (simple_rel_array, simple_rte_array, and append_rel_array) by a specified number of additional entries. The newly added entries are initialized to NULL. The function uses `repalloc0_array` for existing arrays to preserve current data while expanding capacity.

A notable behavior is that this function ensures append_rel_array is allocated even if it wasn't previously, since this function is typically called when adding child relations that require AppendRelInfos.

## Parameters / Member Variables
- `root`: PlannerInfo structure containing the arrays to be expanded
- `add_size`: Number of additional entries to add to each array (must be > 0)

## Dependencies
- Functions called/Symbols referenced:
  - repalloc0_array (for expanding existing arrays while preserving data)
  - palloc0_array (for initial allocation of append_rel_array)
  - Assert (for parameter validation)
- Data structures used:
  - RelOptInfo
  - [RangeTblEntry](../R/RangeTblEntry.md)  
  - [AppendRelInfo](../A/AppendRelInfo.md)
- Called from (representative examples):
  - [expand_inherited_rtentry](expand_inherited_rtentry.md) (src/backend/optimizer/util/inherit.c:184)
  - [expand_partitioned_rtentry](expand_partitioned_rtentry.md) (src/backend/optimizer/util/inherit.c:363)

## Notes and Other Information
- The function maintains data integrity by preserving existing array contents during expansion
- Forces allocation of append_rel_array even if previously NULL, anticipating child relation needs
- Includes assertion to ensure add_size is positive
- Updates simple_rel_array_size to reflect the new capacity
- Primarily used during inheritance expansion and partitioned table processing