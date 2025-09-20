# DiscreteKnapsack

## Location
[src/backend/lib/knapsack.c:52-106](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/lib/knapsack.c#L52-L106)

## Overview
Solves the discrete knapsack problem by selecting a subset of items that maximizes total value while staying within a weight constraint.

## Definition

```c
Bitmapset *
DiscreteKnapsack(int max_weight, int num_items,
				 int *item_weights, double *item_values)
```
## Detailed Description
This function implements the classic discrete knapsack algorithm using dynamic programming. It finds the optimal subset of items to include in a knapsack of limited capacity, maximizing the total value while respecting the weight constraint. The algorithm is optimized to reuse memory by working from larger weights to smaller weights in each pass.

The implementation uses a two-dimensional approach conceptually but optimizes memory usage by maintaining only the current state. For each weight capacity from 0 to max_weight, it tracks both the maximum achievable value and the actual set of items that produces that value using Bitmapsets.

The function creates a temporary memory context to manage allocations and automatically cleans up when complete.

## Parameters / Member Variables
- : Maximum weight capacity of the knapsack
- : Number of items available for selection (must be > 0)
- : Array of weights for each item (required, non-null)
- : Array of values for each item (optional; if null, all items assumed to have value 1.0)

## Dependencies
- Functions called/Symbols referenced:
  - AllocSetContextCreate
  - ALLOCSET_SMALL_SIZES
  - [bms_make_singleton](../b/bms_make_singleton.md)
  - [bms_replace_members](../b/bms_replace_members.md)
  - [bms_add_member](../b/bms_add_member.md)
  - [bms_copy](../b/bms_copy.md)
  - [bms_del_member](../b/bms_del_member.md)
  - [MemoryContextDelete](../M/MemoryContextDelete.md)
- Called from (representative examples):
  - [consider_groupingsets_paths](../c/consider_groupingsets_paths.md) (src/backend/optimizer/plan/planner.c:4485)

## Notes and Other Information
- Uses dynamic programming with O(max_weight * num_items) time complexity
- Memory optimized by reusing arrays and working backwards through weights
- All Bitmapsets are pre-initialized with an unused high bit to minimize memory allocations
- Returns a Bitmapset containing 0-based indices of selected items
- Handles the case where item_values is NULL by treating all items as having unit value
- Creates and destroys a temporary memory context to avoid memory leaks
- Used primarily in query optimization for selecting optimal grouping sets