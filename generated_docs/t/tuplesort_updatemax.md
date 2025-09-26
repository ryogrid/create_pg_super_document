# tuplesort_updatemax

## Location
[src/backend/utils/sort/tuplesort.c:988-1038](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/sort/tuplesort.c#L988-L1038)

## Overview
Internal function that updates the maximum resource usage statistics for a tuplesort operation, tracking peak memory or disk space utilization.

## Definition
```c
static void tuplesort_updatemax(Tuplesortstate *state)
```

## Detailed Description
This function maintains statistics about the peak resource usage during a tuplesort operation. It distinguishes between memory-based and disk-based resource usage, prioritizing disk space tracking when external sorting occurs.

The function calculates current space usage differently based on whether the sort has spilled to disk (has tapeset) or remains in memory. For disk-based sorts, it uses LogicalTapeSetBlocks to determine disk usage. For memory-only sorts, it calculates usage as the difference between allowed and available memory.

The function implements a priority system where disk usage is considered more important than memory usage for tracking purposes. It updates the maximum usage statistics only when current usage exceeds previous maximums, or when transitioning from memory-only to disk-based sorting.

## Parameters / Member Variables
- `state`: Pointer to the Tuplesortstate structure containing current sort state and statistics

## Dependencies
- Functions called/Symbols referenced:
  - [LogicalTapeSetBlocks](../L/LogicalTapeSetBlocks.md) (gets number of disk blocks used by tape set)
  - BLCKSZ (PostgreSQL block size constant)

- Called from (representative examples):
  - [tuplesort_reset](tuplesort_reset.md) (when resetting sort state statistics)
  - [tuplesort_get_stats](tuplesort_get_stats.md) (when gathering final statistics)
  - LEADER (in parallel sort leader context)

## Notes and Other Information
- This is a static internal function not exposed in the public API
- Prioritizes disk usage over memory usage when tracking maximum resource consumption
- Memory tracking becomes less accurate once tuples are returned to the caller due to pfree operations
- The function accounts for the more compact representation of data on disk compared to memory
- Updates three key statistics: maxSpace (peak usage), isMaxSpaceDisk (whether peak was disk-based), and maxSpaceStatus (sort status when peak occurred)
- Disk space is measured in bytes (blocks * BLCKSZ) while memory usage is tracked directly
- Used for performance monitoring and debugging to understand resource consumption patterns
- The priority system ensures that transitioning from memory to disk always updates the maximum, even if disk usage is initially lower than peak memory usage