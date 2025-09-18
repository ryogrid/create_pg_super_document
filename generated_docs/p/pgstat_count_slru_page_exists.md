# pgstat_count_slru_page_exists

## Location
[src/backend/utils/activity/pgstat_slru.c:71-76](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/activity/pgstat_slru.c#L71-L76)

## Overview
Increments the counter for page existence checks in a specific SLRU buffer cache, tracking operations that verify whether pages physically exist on disk.

## Definition
```c
void pgstat_count_slru_page_exists(int slru_idx)
```

## Detailed Description
This function records statistics for page existence verification operations within the SLRU buffer management system. It is called when the system needs to check whether a specific page physically exists on disk without necessarily reading its contents. This type of operation is typically used for validation, metadata operations, or determining whether a page needs to be created versus accessed.

The function increments the `blocks_exists` counter, which helps track how frequently the system performs existence checks. This metric can be useful for understanding access patterns and identifying potential optimization opportunities in scenarios where many existence checks are performed.

## Parameters / Member Variables
- `slru_idx`: Integer index identifying which SLRU instance to update statistics for. This corresponds to a specific SLRU buffer cache.

## Dependencies
- Functions called/Symbols referenced:
  - [get_slru_entry](../g/get_slru_entry.md)
- Called from (representative examples):
  - [SimpleLruDoesPhysicalPageExist](../S/SimpleLruDoesPhysicalPageExist.md)
  - pgstat_count_buffer_hit

## Notes and Other Information
- Page existence checks are distinct from actual page reads or cache hits
- This metric helps track metadata and validation operations on SLRU pages
- Useful for understanding system behavior during page management operations
- Part of PostgreSQL's comprehensive buffer management statistics collection
- Located in src/backend/utils/activity/pgstat_slru.c:71-76