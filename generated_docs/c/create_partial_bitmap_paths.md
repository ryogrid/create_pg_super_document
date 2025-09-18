# create_partial_bitmap_paths

## Location
src/backend/optimizer/path/allpaths.c: 4167 - 4202

## Overview
Creates partial bitmap heap scan paths for parallel execution, enabling bitmap scans to be parallelized across multiple worker processes.

## Definition


## Detailed Description
This function generates partial paths for bitmap heap scans that can be executed in parallel. It evaluates whether parallelization would be beneficial by computing the number of heap pages that would be fetched and determining the optimal number of parallel workers. If parallel execution is worthwhile (more than 0 workers), it creates a bitmap heap path with the specified parallelism and adds it to the relation's partial path list.

The function serves as a bridge between index path creation and parallel execution planning, allowing the optimizer to consider parallelized bitmap scans as an alternative to sequential or other types of parallel scans.

## Parameters / Member Variables
- : PlannerInfo containing global planner state and configuration
- : RelOptInfo for the relation to create bitmap paths for
- : Path representing the bitmap qualification (typically from index scans)

## Dependencies
- Functions called/Symbols referenced:
  - [compute_bitmap_pages](compute_bitmap_pages.md) (calculates expected number of heap pages to fetch)
  - [compute_parallel_worker](compute_parallel_worker.md) (determines optimal number of parallel workers)
  - [add_partial_path](../a/add_partial_path.md) (adds the created path to relation's partial path list)
  - [create_bitmap_heap_path](create_bitmap_heap_path.md) (creates the actual bitmap heap scan path)
- Called from (representative examples):
  - [create_index_paths](create_index_paths.md) (when building index-based access paths)

## Notes and Other Information
- Only creates partial paths when parallel execution is beneficial (parallel_workers > 0)
- Uses a selectivity of 1.0 when computing bitmap pages, assuming all qualifying rows will be fetched
- The created bitmap heap path includes lateral_relids and parallel worker count for proper costing
- Partial paths are specifically designed for parallel query execution and are separate from regular paths
- The function relies on max_parallel_workers_per_gather configuration parameter to limit parallelism