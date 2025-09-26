# IncrementalSort

## Location
[src/include/nodes/plannodes.h:955-959](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/plannodes.h#L955-L959)

## Overview
IncrementalSort is a specialized plan node that optimizes sorting operations by taking advantage of input data that is already partially sorted, reducing memory usage and improving performance compared to full sorting.

## Definition

```c
typedef struct IncrementalSort
{
	Sort		sort;
	int			nPresortedCols; /* number of presorted columns */
} IncrementalSort;
```
## Detailed Description
The IncrementalSort node is an optimized variant of the Sort node that leverages pre-existing order in the input data. Instead of sorting the entire dataset at once, it processes the data in groups based on the presorted columns, sorting only the additional columns within each group. This approach significantly reduces memory consumption and can improve performance when the input is already partially ordered. The node inherits all sorting functionality from the base Sort structure while adding the capability to track how many leading columns are already sorted.

## Parameters / Member Variables
- `sort`: Base Sort structure containing all standard sorting information (inherited)
- `nPresortedCols`: Number of leading columns that are already sorted in the input data
## Dependencies
- Functions called/Symbols referenced:
  - [Sort](../S/Sort.md) (base structure)

- Called from (representative examples):
  - [ExecIncrementalSort](../E/ExecIncrementalSort.md) (executor/nodeIncrementalSort.c:503)
  - [ExecInitIncrementalSort](../E/ExecInitIncrementalSort.md) (executor/nodeIncrementalSort.c:976)
  - [create_incrementalsort_plan](../c/create_incrementalsort_plan.md) (optimizer/plan/createplan.c:2218)
  - [make_incrementalsort](../m/make_incrementalsort.md) (optimizer/plan/createplan.c:6103)
  - [show_incremental_sort_keys](../s/show_incremental_sort_keys.md) (commands/explain.c:2577)
  - [preparePresortedCols](../p/preparePresortedCols.md) (executor/nodeIncrementalSort.c:166)

## Notes and Other Information
- [IncrementalSort](IncrementalSort.md) is particularly beneficial when dealing with multi-column sorts where some leading columns are already ordered
- The node can switch between different execution modes: full sort mode for the first group and incremental sort mode for subsequent groups
- Memory usage is typically much lower than regular Sort since it processes data in smaller chunks
- The optimization is most effective when the presorted columns have low cardinality relative to the total number of rows
- Introduced as a performance optimization in PostgreSQL 13
- The planner automatically chooses IncrementalSort over Sort when it detects beneficial pre-existing order in the input