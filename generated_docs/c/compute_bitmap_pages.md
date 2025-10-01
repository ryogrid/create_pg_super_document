# compute_bitmap_pages

## Location
[src/backend/optimizer/path/costsize.c:6406-6504](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/costsize.c#L6406-L6504)

## Overview
Estimates the number of heap pages that will be fetched during a bitmap heap scan, accounting for memory constraints and lossy page handling.

## Definition
```c
double compute_bitmap_pages(PlannerInfo *root, RelOptInfo *baserel,
                            Path *bitmapqual, double loop_count,
                            Cost *cost_p, double *tuples_p)
```

## Detailed Description
This function estimates how many heap pages will be accessed during a bitmap heap scan by analyzing the bitmap qualification tree and considering memory limitations. It uses the Mackert and Lohman formula to estimate page fetches for a single scan, then adjusts for repeated scans if loop_count > 1. The function also handles the case where the bitmap becomes too large for work_mem, requiring some pages to become 'lossy' (where the entire page is marked rather than individual tuples). The calculation accounts for both exact and lossy pages when determining the final tuple and page estimates.

## Parameters / Member Variables
- `root`: PlannerInfo structure containing query planning context
- `baserel`: RelOptInfo for the relation being scanned
- `bitmapqual`: Path tree representing the bitmap qualification (IndexPaths, BitmapAndPaths, BitmapOrPaths)
- `loop_count`: Number of times the bitmap scan will be repeated (for caching behavior estimates)
- `cost_p`: Output parameter for returning the total index cost (can be NULL)
- `tuples_p`: Output parameter for returning the estimated tuples fetched (can be NULL)

## Dependencies
- Functions called/Symbols referenced:
  - [cost_bitmap_tree_node](cost_bitmap_tree_node.md) (gets cost and selectivity of bitmap qualification)
  - [clamp_row_est](clamp_row_est.md) (ensures row estimates are within reasonable bounds)
  - [tbm_calculate_entries](../t/tbm_calculate_entries.md) (calculates maximum bitmap entries for given memory)
  - [index_pages_fetched](../i/index_pages_fetched.md) (estimates pages fetched accounting for cache effects)
  - [get_indexpath_pages](../g/get_indexpath_pages.md) (gets total index pages for bitmap path)
  - Cost (type for cost estimates)
- Called from (representative examples):
  - [create_partial_bitmap_paths](create_partial_bitmap_paths.md)
  - [cost_bitmap_heap_scan](cost_bitmap_heap_scan.md)

## Notes and Other Information
- This is a public function used by the query optimizer for bitmap scan costing
- Uses the Mackert and Lohman formula for estimating heap page fetches
- Handles memory constraints by calculating when the bitmap becomes lossy
- The lossy page calculation uses a crude but effective approximation
- Critical for accurate bitmap heap scan cost estimation
- Accounts for the reality that bitmap scans may need to access entire pages when memory is limited
- The function carefully handles edge cases like T <= 1 and ensures page estimates don't exceed the total relation size

## Simplified Source

```c
double compute_bitmap_pages(PlannerInfo *root, RelOptInfo *baserel,
                            Path *bitmapqual, double loop_count,
                            Cost *cost_p, double *tuples_p) {
    Cost indexTotalCost;
    Selectivity indexSelectivity;
    double T, pages_fetched, tuples_fetched, heap_pages;
    long maxentries;

    // Get cost and selectivity of the bitmap qualification
    cost_bitmap_tree_node(bitmapqual, &indexTotalCost, &indexSelectivity);

    // Estimate tuples to be fetched
    tuples_fetched = clamp_row_est(indexSelectivity * baserel->tuples);
    T = (baserel->pages > 1) ? (double) baserel->pages : 1.0;

    // Apply Mackert and Lohman formula for single scan
    pages_fetched = (2.0 * T * tuples_fetched) / (2.0 * T + tuples_fetched);

    // Calculate heap pages and bitmap memory limits
    heap_pages = Min(pages_fetched, baserel->pages);
    maxentries = tbm_calculate_entries(work_mem * 1024L);

    // Adjust for multiple scans
    if (loop_count > 1) {
        pages_fetched = index_pages_fetched(tuples_fetched * loop_count,
                                          baserel->pages,
                                          get_indexpath_pages(bitmapqual),
                                          root);
        pages_fetched /= loop_count;
    }

    // Ensure we don't exceed total relation size
    pages_fetched = (pages_fetched >= T) ? T : ceil(pages_fetched);

    // Handle lossy pages when bitmap exceeds memory
    if (maxentries < heap_pages) {
        double exact_pages, lossy_pages;

        // Estimate lossy vs exact pages
        lossy_pages = Max(0, heap_pages - maxentries / 2);
        exact_pages = heap_pages - lossy_pages;

        // Recalculate tuples when lossy pages are present
        if (lossy_pages > 0) {
            tuples_fetched = clamp_row_est(
                indexSelectivity *
                ((exact_pages / heap_pages) * baserel->tuples +
                 (lossy_pages / heap_pages) * baserel->tuples));
        }
    }

    // Return optional output parameters
    if (cost_p)
        *cost_p = indexTotalCost;
    if (tuples_p)
        *tuples_p = tuples_fetched;

    return pages_fetched;
}
```