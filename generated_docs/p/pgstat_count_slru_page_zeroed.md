# pgstat_count_slru_page_zeroed

## Location
src/backend/utils/activity/pgstat_slru.c: 59 - 64

## Overview
Increments the count of zeroed pages for a specific SLRU buffer cache, tracking when new pages are initialized to zero values.

## Definition
```c
void pgstat_count_slru_page_zeroed(int slru_idx)
```

## Detailed Description
This function is part of the SLRU statistics accumulation infrastructure, specifically designed to track page initialization operations. When an SLRU buffer management system needs to create a new page and initialize it with zero values, this function is called to record that event. The function directly increments the `blocks_zeroed` counter for the specified SLRU instance.

This metric is valuable for understanding SLRU buffer allocation patterns and can help identify when new data is being created versus when existing data is being accessed. High zeroed page counts might indicate significant new data creation or buffer cache misses requiring new page allocation.

## Parameters / Member Variables
- `slru_idx`: Integer index identifying which SLRU instance to update statistics for. This corresponds to a specific SLRU buffer cache.

## Dependencies
- Functions called/Symbols referenced:
  - get_slru_entry
- Called from (representative examples):
  - SimpleLruZeroPage
  - pgstat_count_buffer_hit

## Notes and Other Information
- This is one of the SLRU statistics count accumulation functions called from slru.c
- The function performs a simple atomic increment operation on the blocks_zeroed counter
- Part of PostgreSQL's comprehensive buffer management statistics collection
- Located in src/backend/utils/activity/pgstat_slru.c:59-64