# pgstat_count_slru_page_read

## Location
[src/backend/utils/activity/pgstat_slru.c:77-82](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/activity/pgstat_slru.c#L77-L82)

## Overview
Increments the disk read counter for a specific SLRU buffer cache, tracking when pages must be physically read from disk storage due to cache misses.

## Definition
```c
void pgstat_count_slru_page_read(int slru_idx)
```

## Detailed Description
This function records disk read operations for SLRU (Simple Least Recently Used) buffer management. It is called when a requested page is not found in the buffer cache and must be physically read from disk storage. This represents a cache miss scenario and is generally more expensive than cache hits due to the I/O overhead involved.

The function increments the `blocks_read` counter for the specified SLRU instance. This metric is crucial for performance analysis as it indicates the frequency of expensive disk I/O operations. When analyzed together with cache hit statistics, it helps calculate cache hit ratios and identify opportunities for buffer tuning or access pattern optimization.

## Parameters / Member Variables
- `slru_idx`: Integer index identifying which SLRU instance to update statistics for. This corresponds to a specific SLRU buffer cache.

## Dependencies
- Functions called/Symbols referenced:
  - [get_slru_entry](../g/get_slru_entry.md)
- Called from (representative examples):
  - [SimpleLruReadPage](../S/SimpleLruReadPage.md)
  - pgstat_count_buffer_hit

## Notes and Other Information
- Represents cache miss events requiring expensive disk I/O operations
- Used in conjunction with hit counters to calculate cache hit ratios
- High read counts may indicate insufficient buffer cache size or poor data locality
- Essential metric for SLRU performance monitoring and tuning decisions
- Part of PostgreSQL's comprehensive buffer management statistics collection
- Located in src/backend/utils/activity/pgstat_slru.c:77-82

## Simplified Source

```c
void pgstat_count_slru_page_read(int slru_idx)
{
    // Increment the disk read counter for this SLRU
    get_slru_entry(slru_idx)->blocks_read += 1;
}
```