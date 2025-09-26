# check_for_freed_segments_locked

## Location
src/backend/utils/mmgr/dsa.c: 2288 - 2315

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
  - LWLockHeldByMe (assertion to verify lock is held)
  - DSA_AREA_LOCK (macro to get area-specific lock)
  - dsm_detach (detaches dynamic shared memory segment)
- Called from (representative examples):
  - get_segment_index
  - dsa_dump
  - destroy_superblock
  - get_best_segment
  - check_for_freed_segments

## Notes and Other Information
- This is a static internal function not exposed in the public API
- Requires the DSA area lock to be held before calling (enforced by assertion)
- Uses `unlikely()` hint for branch prediction optimization on the counter comparison
- Clears all mapping fields (segment, header, mapped_address) when detaching freed segments
- Critical for maintaining memory safety in multi-process DSA usage scenarios