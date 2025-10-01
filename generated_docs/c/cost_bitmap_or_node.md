# cost_bitmap_or_node

## Location
[src/backend/optimizer/path/costsize.c:1201-1248](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/costsize.c#L1201-L1248)

## Overview
Estimates the cost of a BitmapOr node by calculating the combined costs of its constituent bitmap operations and the bitmap union overhead.

## Definition
```c
void cost_bitmap_or_node(BitmapOrPath *path, PlannerInfo *root)
```

## Detailed Description
This function computes the cost and selectivity for a BitmapOr node, which represents the union of multiple bitmap index scans. The costing model includes:

1. **Selectivity Calculation**: Adds the selectivities of all child nodes, assuming non-overlapping conditions (typical in "x IN (list)" scenarios), then clamps the result to 1.0 maximum
2. **Base Costs**: Sums up the costs of all constituent bitmap operations by calling cost_bitmap_tree_node for each child
3. **Union Overhead**: Adds 100 * cpu_operator_cost for each tbm_union operation, but only for non-IndexPath children (since BitmapIndexScan unions are optimized out)

Like cost_bitmap_and_node, this function treats the BitmapOrPath as a pseudo-Path object with cost properties but doesn't populate the rows field since it represents an intermediate bitmap operation rather than a complete scan path.

## Parameters / Member Variables
- `path`: The BitmapOrPath node whose cost is being calculated
- `root`: The PlannerInfo containing global planning context (currently unused but maintained for interface consistency)

## Dependencies
- Functions called/Symbols referenced:
  - [cost_bitmap_tree_node](cost_bitmap_tree_node.md) (to get cost/selectivity from child nodes)
  - [list_head](../l/list_head.md) (to check for first element in list)
  - [IndexPath](../I/IndexPath.md) (struct type for optimization check)
  - Min (macro to clamp selectivity)
  - cpu_operator_cost (global cost parameter)

- Called from:
  - [create_bitmap_or_path](create_bitmap_or_path.md) (in pathnode.c:1169)

## Notes and Other Information
- Assumes non-overlapping selectivities, which is often true for "x IN (list)" patterns
- Optimizes away union costs for BitmapIndexScan inputs since they are handled differently
- Uses the same simplistic cost model as BitmapAnd (100 * cpu_operator_cost per operation)
- Clamps final selectivity to 1.0 to prevent impossible estimates
- Does not set the rows field as this is an intermediate bitmap operation
- Located in src/backend/optimizer/path/costsize.c:1201-1248

## Simplified Source

```c
void cost_bitmap_or_node(BitmapOrPath *path, PlannerInfo *root)
{
    Cost totalCost = 0.0;
    Selectivity selec = 0.0;
    ListCell *l;

    // Process each child bitmap path
    foreach(l, path->bitmapquals)
    {
        Path *subpath = (Path *) lfirst(l);
        Cost subCost;
        Selectivity subselec;

        // Get cost and selectivity of child path
        cost_bitmap_tree_node(subpath, &subCost, &subselec);

        // Add selectivities (assume non-overlapping)
        selec += subselec;

        // Add child cost
        totalCost += subCost;

        // Add union cost (100 * cpu_operator_cost per union)
        // Skip for IndexPath since unions are optimized out
        if (l != list_head(path->bitmapquals) && !IsA(subpath, IndexPath))
            totalCost += 100.0 * cpu_operator_cost;
    }

    // Set final cost and selectivity (clamp selectivity to 1.0)
    path->bitmapselectivity = Min(selec, 1.0);
    path->path.rows = 0;            // Not used for bitmap operations
    path->path.startup_cost = totalCost;
    path->path.total_cost = totalCost;
}
```