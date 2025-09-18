# pgstat_count_slru_page_hit

## Location
[src/backend/utils/activity/pgstat_slru.c:65-70](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/activity/pgstat_slru.c#L65-L70)

## Overview
Increments the cache hit counter for a specific SLRU buffer cache, tracking successful page retrievals from memory without requiring disk I/O.

## Definition
```c
void pgstat_count_slru_page_hit(int slru_idx)
```

## Detailed Description
This function records cache hit events for SLRU (Simple Least Recently Used) buffer management. A cache hit occurs when a requested page is found in the SLRU buffer cache, meaning the data can be accessed directly from memory without performing expensive disk read operations. This is a critical performance metric as cache hits indicate efficient buffer utilization and good data locality.

The function simply increments the `blocks_hit` counter for the specified SLRU instance. High cache hit ratios typically indicate good performance, while low hit ratios may suggest the need for buffer tuning or indicate access patterns that don't benefit from caching.

## Parameters / Member Variables
- `slru_idx`: Integer index identifying which SLRU instance to update statistics for. This corresponds to a specific SLRU buffer cache.

## Dependencies
- Functions called/Symbols referenced:
  - [get_slru_entry](../g/get_slru_entry.md)
- Called from (representative examples):
  - [SimpleLruReadPage](../S/SimpleLruReadPage.md)
  - [SimpleLruReadPage_ReadOnly](../S/SimpleLruReadPage_ReadOnly.md)
  - pgstat_count_buffer_hit

## Notes and Other Information
- Cache hit tracking is essential for SLRU performance monitoring and tuning
- High hit ratios indicate effective buffer cache utilization
- This metric is often used in conjunction with read counters to calculate hit ratios
- Part of PostgreSQL's comprehensive buffer management statistics collection
- Located in src/backend/utils/activity/pgstat_slru.c:65-70