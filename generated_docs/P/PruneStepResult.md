# PruneStepResult

## Location
src/backend/partitioning/partprune.c: 127 - 137

## Overview
PruneStepResult represents the outcome of executing a single partition pruning step, containing information about which partitions should be included in the scan.

## Definition
```c
typedef struct PruneStepResult
{
    /*
     * The offsets of bounds (in a table's boundinfo) whose partition is
     * selected by the pruning step.
     */
    Bitmapset  *bound_offsets;

    bool        scan_default;   /* Scan the default partition? */
    bool        scan_null;      /* Scan the partition for NULL values? */
} PruneStepResult;
```

## Detailed Description
PruneStepResult is a lightweight structure that encapsulates the results of performing one PartitionPruneStep during PostgreSQL's partition elimination process, defined in src/backend/partitioning/partprune.c:127-137. This structure represents the outcome of evaluating a pruning condition against a partitioned table's metadata, determining which specific partitions need to be included in the query execution.

The structure uses a bitmap-based approach to efficiently represent which partition bounds are selected, along with explicit flags for special cases like default and NULL-value partitions. This design allows for efficient set operations when combining results from multiple pruning steps and provides a compact representation of potentially large partition sets.

The bound_offsets field is the core result, containing a bitmap where each bit corresponds to a bound offset in the table's partition boundary information. The boolean flags handle edge cases that require special treatment in the partitioning system.

## Parameters / Member Variables
- `bound_offsets`: A Bitmapset containing the offsets of bounds (in the table's boundinfo structure) whose corresponding partitions are selected by the pruning step
- `scan_default`: Boolean flag indicating whether the default partition (if it exists) should be included in the scan
- `scan_null`: Boolean flag indicating whether the partition designated for NULL values should be included in the scan

## Dependencies
- Functions called/Symbols referenced:
  - Bitmapset (PostgreSQL bitmap data structure)

- Called from (representative examples):
  - get_matching_partitions
  - get_steps_using_prefix_recurse
  - get_matching_hash_bounds
  - get_matching_list_bounds
  - get_matching_range_bounds
  - get_partkey_exec_paramids
  - perform_pruning_base_step
  - perform_pruning_combine_step

## Notes and Other Information
- This structure is the fundamental result type returned by partition pruning operations, providing a standardized way to represent which partitions should be scanned
- The bitmap-based representation using bound_offsets allows for efficient set operations when combining results from multiple pruning steps (union, intersection, etc.)
- The separate boolean flags for default and NULL partitions are necessary because these special partitions may not follow the normal partitioning bounds logic
- The structure is used extensively in both hash and range partitioning schemes, making it a universal result type across different partitioning strategies
- Results from multiple PruneStepResult structures can be combined using set operations to implement complex AND/OR logic in partition elimination
- The bound_offsets bitmap directly corresponds to entries in the partition's boundinfo structure, providing an efficient mapping between pruning results and actual partition identifiers
- This design enables PostgreSQL's partition-wise joins and other advanced optimizations by providing a clear, efficient representation of which partitions contain potentially matching data