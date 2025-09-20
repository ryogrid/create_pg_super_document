# pgstat_count_slru_flush

## Location
[src/backend/utils/activity/pgstat_slru.c:89-94](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/activity/pgstat_slru.c#L89-L94)

## Overview
Increments the flush counter for a specific SLRU (Simple LRU) buffer cache to track statistics about flush operations.

## Definition

```c
void
pgstat_count_slru_flush(int slru_idx)
```
## Detailed Description
This function is part of PostgreSQL's statistics collection system for SLRU (Simple LRU) buffer caches. It increments the flush counter for the specified SLRU cache index by 1. SLRU flush operations occur when multiple dirty pages need to be written to disk simultaneously, typically during checkpoint operations or when the cache needs to free up space. This function helps track the frequency of flush operations for performance monitoring and tuning.

The function operates by calling get_slru_entry() to retrieve the statistics entry for the given SLRU index and then atomically incrementing the flush field.

## Parameters / Member Variables
- : Integer index identifying which SLRU cache to update flush statistics for (must be between 0 and SLRU_NUM_ELEMENTS-1)

## Dependencies
- Functions called/Symbols referenced:
  - [get_slru_entry](../g/get_slru_entry.md)
- Called from (representative examples):
  - [SimpleLruWriteAll](../S/SimpleLruWriteAll.md)
  - pgstat_count_buffer_hit

## Notes and Other Information
- This function is called during batch write operations when multiple SLRU pages are flushed together
- Flush operations are typically more expensive than single page writes, so tracking them separately provides valuable performance insights
- The statistics help database administrators understand SLRU cache behavior and identify potential I/O bottlenecks
- Part of the PostgreSQL statistics collector subsystem for comprehensive buffer cache monitoring