# dsm_pin_segment

## Location
[src/backend/storage/ipc/dsm.c:955-987](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/ipc/dsm.c#L955-L987)

## Overview
Prevents a dynamic shared memory segment from being destroyed by keeping it alive until postmaster shutdown or until explicitly unpinned, even when no sessions are attached to it.

## Definition
```c
void dsm_pin_segment(dsm_segment *seg)
```

## Detailed Description
The dsm_pin_segment function ensures the persistence of a dynamic shared memory segment beyond the lifetime of any individual session or process attachment. It works by incrementing the segment's reference count in the shared control structure and marking it as pinned. This prevents the segment from being automatically destroyed when the last process detaches from it.

The function operates under the DynamicSharedMemoryControlLock to ensure atomic updates to the control structure. For non-main-region segments, it also calls the implementation-specific dsm_impl_pin_segment function to perform any additional platform-specific pinning operations.

This is different from dsm_pin_mapping, which only affects the current process's mapping lifetime. dsm_pin_segment affects the segment's global lifetime across all processes.

## Parameters / Member Variables
- `seg`: Pointer to the dsm_segment structure representing the dynamic shared memory segment to be pinned globally

## Dependencies
- Functions called/Symbols referenced:
  - [LWLockAcquire](../L/LWLockAcquire.md)/LWLockRelease (with DynamicSharedMemoryControlLock)
  - [is_main_region_dsm_handle](../i/is_main_region_dsm_handle.md)
  - [dsm_impl_pin_segment](dsm_impl_pin_segment.md)
  - [dsm_segment](dsm_segment.md) (structure type)
  - dsm_control (global control structure)
- Called from (representative examples):
  - [GetNamedDSMSegment](../G/GetNamedDSMSegment.md) (src/backend/storage/ipc/dsm_registry.c:164)
  - [dsa_create_ext](dsa_create_ext.md) (src/backend/utils/mmgr/dsa.c:438)
  - [make_new_segment](../m/make_new_segment.md) (src/backend/utils/mmgr/dsa.c:2181)

## Notes and Other Information
- Should not be called more than once per segment unless explicitly unpinned first (will throw ERROR)
- The function checks dsm_control->item[seg->control_slot].pinned to prevent double-pinning
- Does not affect individual process mappings - use dsm_pin_mapping() to keep mappings alive in specific processes
- Increments the segment's reference count to prevent automatic cleanup
- For main region segments, platform-specific pinning is skipped
- Critical for implementing long-lived shared data structures that outlast individual sessions

## Simplified Source

```c
void
dsm_pin_segment(dsm_segment *seg)
{
    void *handle = NULL;

    // Acquire lock to safely modify shared control structure
    LWLockAcquire(DynamicSharedMemoryControlLock, LW_EXCLUSIVE);

    // Check if already pinned (prevent double-pinning)
    if (dsm_control->item[seg->control_slot].pinned)
        elog(ERROR, "cannot pin a segment that is already pinned");

    // Call platform-specific pinning if needed
    if (!is_main_region_dsm_handle(seg->handle))
        dsm_impl_pin_segment(seg->handle, seg->impl_private, &handle);

    // Mark as pinned and increment reference count
    dsm_control->item[seg->control_slot].pinned = true;
    dsm_control->item[seg->control_slot].refcnt++;
    dsm_control->item[seg->control_slot].impl_private_pm_handle = handle;

    LWLockRelease(DynamicSharedMemoryControlLock);
}
```

**Simplified Explanation:**
1. Acquire exclusive lock on DSM control structure
2. Check that segment isn't already pinned (error if it is)
3. Call platform-specific pinning for non-main-region segments
4. Mark segment as pinned and increment its reference count
5. Store any implementation-specific handle and release lock