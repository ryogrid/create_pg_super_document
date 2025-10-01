# bitmap_scan_cost_est

## Location
[src/backend/optimizer/path/indxpath.c:1526-1559](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/indxpath.c#L1526-L1559)

## Overview
Estimates the cost of executing a bitmap scan with a single index path, which could be a BitmapAnd or BitmapOr node, by creating a dummy BitmapHeapPath and calculating its cost.

## Definition
```c
static Cost bitmap_scan_cost_est(PlannerInfo *root, RelOptInfo *rel, Path *ipath)
```

## Detailed Description
This static function provides a cost estimation for bitmap scan execution by setting up a temporary BitmapHeapPath structure and using the existing cost_bitmap_heap_scan() function to calculate the total cost. The function creates a dummy BitmapHeapPath with the provided index path as the bitmap qualifier, then calls the standard costing function to get an accurate cost estimate.

The function explicitly disables parallelism (sets parallel_workers to 0) to get a baseline cost estimate without parallel processing considerations, as parallel bitmap heap paths are evaluated separately at a later stage in the planning process.

## Parameters / Member Variables
- `root`: PlannerInfo structure containing planning context and global information
- `rel`: RelOptInfo structure representing the relation being scanned
- `ipath`: Path structure representing the index path (could be BitmapAnd or BitmapOr)

## Dependencies
- Functions called/Symbols referenced:
  - [cost_bitmap_heap_scan](../c/cost_bitmap_heap_scan.md)
  - [get_loop_count](../g/get_loop_count.md)
  - PATH_REQ_OUTER
  - [BitmapHeapPath](../B/BitmapHeapPath.md)
  - Cost
- Called from (representative examples):
  - [choose_bitmap_and](../c/choose_bitmap_and.md)
  - [bitmap_and_cost_est](bitmap_and_cost_est.md)

## Notes and Other Information
- This is a static function local to indxpath.c
- Creates a temporary BitmapHeapPath structure purely for cost estimation purposes
- Explicitly sets parallel_workers to 0 to exclude parallelism from the cost calculation
- Uses the rel->reltarget as the path target and inherits param_info from the input index path
- Returns only the total_cost field from the calculated BitmapHeapPath
- Part of PostgreSQL's cost-based optimization system for bitmap index scans

## Simplified Source

```c
static Cost
bitmap_scan_cost_est(PlannerInfo *root, RelOptInfo *rel, Path *ipath)
{
    BitmapHeapPath bpath;

    // Set up a dummy BitmapHeapPath for cost calculation
    bpath.path.type = T_BitmapHeapPath;
    bpath.path.pathtype = T_BitmapHeapScan;
    bpath.path.parent = rel;
    bpath.path.pathtarget = rel->reltarget;
    bpath.path.param_info = ipath->param_info;
    bpath.path.pathkeys = NIL;
    bpath.bitmapqual = ipath;

    // Disable parallelism for base cost calculation
    bpath.path.parallel_workers = 0;

    // Calculate the cost using standard bitmap heap scan costing
    cost_bitmap_heap_scan(&bpath.path, root, rel,
                          bpath.path.param_info,
                          ipath,
                          get_loop_count(root, rel->relid, PATH_REQ_OUTER(ipath)));

    return bpath.path.total_cost;
}
```