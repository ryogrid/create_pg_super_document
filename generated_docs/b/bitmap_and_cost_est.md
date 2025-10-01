# bitmap_and_cost_est

## Location
[src/backend/optimizer/path/indxpath.c:1560-1588](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/indxpath.c#L1560-L1588)

## Overview
Estimates the cost of executing a BitmapAnd scan with the given input paths by creating a real BitmapAndPath and delegating to bitmap_scan_cost_est for cost calculation.

## Definition
```c
static Cost bitmap_and_cost_est(PlannerInfo *root, RelOptInfo *rel, List *paths)
```

## Detailed Description
This static function provides cost estimation for BitmapAnd operations by actually constructing a BitmapAndPath structure and then using the bitmap_scan_cost_est function to calculate the total cost. Rather than duplicating the complex cost calculation logic, it leverages the existing infrastructure by creating a real BitmapAndPath object and passing it to the general bitmap scan cost estimator.

The function chooses to create an actual BitmapAndPath rather than just performing inline cost calculations because the logic involved is complex enough that replicating it would not be worth the savings of a single palloc operation.

## Parameters / Member Variables
- `root`: PlannerInfo structure containing planning context and global information  
- `rel`: RelOptInfo structure representing the relation being scanned
- `paths`: List of Path structures that will be combined with BitmapAnd operation

## Dependencies
- Functions called/Symbols referenced:
  - [create_bitmap_and_path](../c/create_bitmap_and_path.md)
  - [bitmap_scan_cost_est](bitmap_scan_cost_est.md)
  - [BitmapAndPath](../B/BitmapAndPath.md)
  - PathClauseUsage
- Called from (representative examples):
  - [choose_bitmap_and](../c/choose_bitmap_and.md)

## Notes and Other Information
- This is a static function local to indxpath.c
- Creates an actual BitmapAndPath rather than just estimating costs inline, due to the complexity of the cost calculation
- Delegates the actual cost calculation to bitmap_scan_cost_est after creating the BitmapAndPath
- Part of PostgreSQL's cost-based optimization for bitmap index scans with AND operations
- The comment indicates that the cost calculation is complex enough that avoiding duplication of logic justifies the overhead of creating the actual path structure

## Simplified Source

```c
static Cost
bitmap_and_cost_est(PlannerInfo *root, RelOptInfo *rel, List *paths)
{
    BitmapAndPath *apath;

    // Create a real BitmapAndPath for cost calculation
    // (too complex to duplicate the cost logic inline)
    apath = create_bitmap_and_path(root, rel, paths);

    // Delegate to the general bitmap scan cost estimator
    return bitmap_scan_cost_est(root, rel, (Path *) apath);
}
```