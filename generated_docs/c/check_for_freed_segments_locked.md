# check_for_freed_segments_locked

## Location
[src/backend/utils/mmgr/dsa.c:2288-2315](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mmgr/dsa.c#L2288-L2315)

## Overview
Internal workhorse function that unmaps stale segment mappings from a DSA area while holding the area lock, ensuring segment index consistency before lookups.

## Definition
```c
static void check_for_freed_segments_locked(dsa_area *area)
```

## Detailed Description
This function serves as the core implementation for detecting and cleaning up freed segments in a Dynamic Shared Area (DSA). It operates under the assumption that the caller already holds the DSA area lock, making it suitable for use in critical paths where lock acquisition overhead should be minimized.

The function compares the local freed segment counter with the shared control structure counter to detect if any segments have been freed by other processes. When a mismatch is detected, it iterates through all mapped segments and detaches any that have been marked as freed, clearing their local mapping information to prevent stale references.

This cleanup is essential before performing segment index lookups to avoid accessing memory that may have been reallocated with the same index but different content.

## Parameters / Member Variables
- `area`: Pointer to the DSA area structure containing segment mappings and control information

## Dependencies
- Functions called/Symbols referenced:
  - [LWLockHeldByMe](../L/LWLockHeldByMe.md) (assertion to verify lock is held)
  - DSA_AREA_LOCK (macro to get area-specific lock)
  - [dsm_detach](../d/dsm_detach.md) (detaches dynamic shared memory segment)
- Called from (representative examples):
  - get_segment_index
  - [dsa_dump](../d/dsa_dump.md)
  - [destroy_superblock](../d/destroy_superblock.md)
  - [get_best_segment](../g/get_best_segment.md)
  - [check_for_freed_segments](check_for_freed_segments.md)

## Notes and Other Information
- This is a static internal function not exposed in the public API
- Requires the DSA area lock to be held before calling (enforced by assertion)
- Uses `unlikely()` hint for branch prediction optimization on the counter comparison
- Clears all mapping fields (segment, header, mapped_address) when detaching freed segments
- Critical for maintaining memory safety in multi-process DSA usage scenarios

## Simplified Source

```c
static void
check_for_freed_segments_locked(dsa_area *area)
{
    size_t freed_segment_counter;
    int i;

    // Verify we hold the area lock
    Assert(LWLockHeldByMe(DSA_AREA_LOCK(area)));

    // Check if any segments were freed by other processes
    freed_segment_counter = area->control->freed_segment_counter;
    if (unlikely(area->freed_segment_counter != freed_segment_counter))
    {
        // Scan all segments and detach any that were freed
        for (i = 0; i <= area->high_segment_index; ++i)
        {
            if (area->segment_maps[i].header != NULL &&
                area->segment_maps[i].header->freed)
            {
                // Clean up the freed segment mapping
                dsm_detach(area->segment_maps[i].segment);
                area->segment_maps[i].segment = NULL;
                area->segment_maps[i].header = NULL;
                area->segment_maps[i].mapped_address = NULL;
            }
        }
        // Update our local counter to match
        area->freed_segment_counter = freed_segment_counter;
    }
}
```