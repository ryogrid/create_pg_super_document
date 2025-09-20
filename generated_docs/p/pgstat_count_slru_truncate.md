# pgstat_count_slru_truncate

## Location
[src/backend/utils/activity/pgstat_slru.c:95-104](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/activity/pgstat_slru.c#L95-L104)

## Overview
Increments the truncate counter for a specific SLRU (Simple LRU) buffer cache to track statistics about truncation operations.

## Definition

```c
void
pgstat_count_slru_truncate(int slru_idx)
```
## Detailed Description
This function is part of PostgreSQL's statistics collection system for SLRU (Simple LRU) buffer caches. It increments the truncate counter for the specified SLRU cache index by 1. SLRU truncation operations occur when older pages in the cache are no longer needed and can be removed to free up space, typically during log cleanup operations. This function helps track the frequency of truncation operations for performance monitoring and understanding cache lifecycle patterns.

The function operates by calling get_slru_entry() to retrieve the statistics entry for the given SLRU index and then atomically incrementing the truncate field.

## Parameters / Member Variables
- : Integer index identifying which SLRU cache to update truncation statistics for (must be between 0 and SLRU_NUM_ELEMENTS-1)

## Dependencies
- Functions called/Symbols referenced:
  - [get_slru_entry](../g/get_slru_entry.md)
  - [PgStat_SLRUStats](../P/PgStat_SLRUStats.md)
- Called from (representative examples):
  - [SimpleLruTruncate](../S/SimpleLruTruncate.md)
  - pgstat_count_buffer_hit

## Notes and Other Information
- This function is called during cleanup operations when SLRU pages are being discarded
- Truncation operations help manage disk space usage by removing outdated log entries
- The statistics provide insights into how frequently different SLRU caches need cleanup
- Frequent truncations might indicate high transaction volume or aggressive cleanup policies
- Part of the PostgreSQL statistics collector subsystem for comprehensive buffer cache monitoring