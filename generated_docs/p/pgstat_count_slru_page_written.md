# pgstat_count_slru_page_written

## Location
[src/backend/utils/activity/pgstat_slru.c:83-88](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/activity/pgstat_slru.c#L83-L88)

## Overview
Increments the blocks_written counter for a specific SLRU (Simple LRU) buffer cache to track statistics about page writes.

## Definition

```c
void
pgstat_count_slru_page_written(int slru_idx)
```
## Detailed Description
This function is part of PostgreSQL's statistics collection system for SLRU (Simple LRU) buffer caches. It increments the blocks_written counter for the specified SLRU cache index by 1. SLRU caches are used for various PostgreSQL subsystems like transaction logs, commit logs, multixact logs, etc. This function helps track I/O activity by counting how many pages have been written to disk for each SLRU cache.

The function operates by calling get_slru_entry() to retrieve the statistics entry for the given SLRU index and then atomically incrementing the blocks_written field.

## Parameters / Member Variables
- `slru_idx`: Integer index identifying which SLRU cache to update statistics for (must be between 0 and SLRU_NUM_ELEMENTS-1)

## Dependencies
- Functions called/Symbols referenced:
  - [get_slru_entry](../g/get_slru_entry.md)
- Called from (representative examples):
  - [SlruPhysicalWritePage](../S/SlruPhysicalWritePage.md)
  - pgstat_count_buffer_hit

## Notes and Other Information
- This function is designed to be lightweight and fast since it's called during I/O operations
- The statistics collected are used for monitoring and performance analysis of SLRU caches
- The function assumes the caller has validated the slru_idx parameter
- Part of the PostgreSQL statistics collector subsystem for tracking buffer cache performance

## Simplified Source
```c
void pgstat_count_slru_page_written(int slru_idx) {
    // Get the statistics entry for the specified SLRU cache
    // and increment the blocks written counter
    get_slru_entry(slru_idx)->blocks_written += 1;
}
```