# GatherMergePath

## Location
src/include/nodes/pathnodes.h: 2053 - 2058

## Overview
GatherMergePath represents a path node for parallel query execution that runs multiple copies of a plan in parallel and collects the results while preserving their common sort order.

## Definition
```c
typedef struct GatherMergePath
{
    Path        path;
    Path       *subpath;        /* path for each worker */
    int         num_workers;    /* number of workers sought to help */
} GatherMergePath;
```

## Detailed Description
GatherMergePath is a specialized parallel execution path node that extends the concept of parallel processing by maintaining sort order across worker outputs. Unlike the basic GatherPath which simply collects results from workers, GatherMergePath performs an ordered merge of the sorted results from multiple parallel workers, ensuring the final output maintains the required sort order.

This path type is particularly valuable for queries that need sorted results and can benefit from parallel execution, such as large table scans with ORDER BY clauses or index scans that can be parallelized while preserving ordering. The merge operation is performed efficiently by treating each worker output as a sorted stream and merging them using a priority queue or similar mechanism.

## Parameters / Member Variables
- `path`: Base Path structure containing standard path information including expected sort order and costs
- `subpath`: Pointer to the Path that will be executed by each worker process, which must produce sorted output
- `num_workers`: The number of worker processes requested to help execute this path

## Dependencies
- Functions called/Symbols referenced:
  - Path (base structure)

- Called from (representative examples):
  - generate_gather_paths (creates parallel path alternatives)
  - generate_useful_gather_paths (creates ordered parallel paths)
  - cost_gather_merge (calculates merge costs)
  - create_gather_merge_plan (converts to execution plan)
  - create_gather_merge_path (path creation function)
  - create_unique_path (for unique ordered results)

## Notes and Other Information
- GatherMergePath requires that the subpath produces sorted output compatible with the desired final ordering
- The merge operation adds overhead compared to simple gathering, but preserves sort order without requiring a separate sort step
- Cost estimation includes both parallel execution benefits and merge coordination overhead
- Unlike GatherPath, there is no single_copy flag since the leader always participates in the merge coordination
- The effectiveness depends heavily on the degree of parallelism and the cost of the merge operation relative to the subpath execution
- Commonly used with parallel index scans and sorted table scans