# setup_simple_rel_arrays

## Location
[src/backend/optimizer/util/relnode.c:94-162](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/relnode.c#L94-L162)

## Overview
Initializes and prepares the arrays used by the PostgreSQL query planner for quickly accessing base relations and AppendRelInfos.

## Definition

```c
void
setup_simple_rel_arrays(PlannerInfo *root)
```
## Detailed Description
This function sets up three critical arrays within the PlannerInfo structure that enable efficient access to relation information during query planning:

1. **simple_rel_array**: An array of RelOptInfo pointers initialized to NULL, later populated by  calls
2. **simple_rte_array**: An array equivalent of the rtable list for faster indexed access to RangeTblEntry structures
3. **append_rel_array**: An array for AppendRelInfo structures, only created when append relations exist

The arrays are sized based on the range table length plus one (to accommodate 1-based RT indexes). The function also handles pre-existing AppendRelInfos from UNION ALL flattening operations.

## Parameters / Member Variables
- : PlannerInfo structure containing query planning state and the arrays to be initialized

## Dependencies
- Functions called/Symbols referenced:
  - [list_length](../l/list_length.md) (to determine array size)
  - [palloc0](../p/palloc0.md) (for memory allocation)
  - lfirst, lfirst_node (for list traversal)
  - elog (for error reporting)
- Data structures used:
  - [AppendRelInfo](../A/AppendRelInfo.md)
  - [RangeTblEntry](../R/RangeTblEntry.md)
  - [RelOptInfo](../R/RelOptInfo.md)
- Called from (representative examples):
  - [query_planner](../q/query_planner.md) (src/backend/optimizer/plan/planmain.c:85)
  - [plan_cluster_use_sort](../p/plan_cluster_use_sort.md) (src/backend/optimizer/plan/planner.c:6784)
  - [plan_create_index_workers](../p/plan_create_index_workers.md) (src/backend/optimizer/plan/planner.c:6914)
  - [plan_set_operations](../p/plan_set_operations.md) (src/backend/optimizer/prep/prepunion.c:133)

## Notes and Other Information
- Arrays use 1-based indexing to match PostgreSQL's range table index convention
- The append_rel_array is conditionally created only when append_rel_list is non-empty, optimizing memory usage
- Includes sanity checks to prevent duplicate child relations in append_rel_array
- This function is typically called early in the planning process to establish the foundational data structures for relation access