# select_active_windows

## Location
[src/backend/optimizer/plan/planner.c:5924-6011](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/planner.c#L5924-L6011)

## Overview
Creates a list of "active" window clauses (those referenced by non-deleted WindowFuncs) in the order they should be executed for optimal performance.

## Definition
```c
static List *select_active_windows(PlannerInfo *root, WindowFuncLists *wflists)
```

## Detailed Description
This function filters window clauses to identify only those that are actively used (have associated WindowFuncs) and sorts them in an execution order that minimizes the need for additional sorting operations. The sorting strategy implements specific SQL standard requirements and optimization goals.

The function performs several key operations:
1. Filters out window clauses that have no associated window functions
2. Constructs unique ordering specifications by combining partition and order clauses while removing duplicates
3. Sorts the active windows using common_prefix_cmp to group windows with compatible sorting requirements
4. Ensures SQL standard compliance for order-equivalent windows
5. Optimizes execution by placing windows with stronger sorting requirements first

The sorting approach ensures that windows with identical partitioning and ordering requirements are grouped together, minimizing sort operations during execution and maintaining SQL standard compliance for peer row ordering.

## Parameters / Member Variables
- `root`: PlannerInfo structure containing the parsed query with window clauses
- `wflists`: WindowFuncLists structure containing window function arrays indexed by window reference

## Dependencies
- Functions called/Symbols referenced:
  - WindowFuncLists, WindowClause (struct types)
  - WindowClauseSortData (struct type for sorting)
  - [list_concat_unique](../l/list_concat_unique.md) (removes duplicates while concatenating)
  - [list_copy](../l/list_copy.md) (creates list copy)
  - [common_prefix_cmp](../c/common_prefix_cmp.md) (comparison function for sorting)
  - qsort (standard library sorting function)
- Called from (representative examples):
  - standard_qp_extra (src/backend/optimizer/plan/planner.c:213)
  - [grouping_planner](../g/grouping_planner.md) (src/backend/optimizer/plan/planner.c:1504)

## Notes and Other Information
- Implements SQL standard General Rule 4 for window clause ordering to ensure peer rows appear in the same order across order-equivalent windows
- Uses a prefix-based sorting strategy where windows with stronger sorting requirements are executed first
- Removes duplicate entries between partition and order clauses to optimize pathkey construction
- The function balances SQL compliance requirements with execution efficiency
- Static function scope indicates internal use within the planner module
- Memory management includes proper allocation and deallocation of the temporary actives array
- The uniqueOrder field in WindowClauseSortData contains the combined, deduplicated sorting specification used for comparison