# dsm_unpin_segment

## Location
[src/backend/storage/ipc/dsm.c:988-1075](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/ipc/dsm.c#L988-L1075)

## Overview
Reverses the effect of dsm_pin_segment by allowing a previously pinned dynamic shared memory segment to be destroyed when no longer referenced, potentially triggering immediate cleanup if this was the last reference.

## Definition
```c
void dsm_unpin_segment(dsm_handle handle)
```

## Detailed Description
The dsm_unpin_segment function removes the "pin" from a dynamic shared memory segment that was previously pinned with dsm_pin_segment, allowing it to be destroyed when no sessions are attached. The function takes a dsm_handle rather than a dsm_segment pointer, making it possible to unpin segments that the current process hasn't mapped.

The function performs several key operations under DynamicSharedMemoryControlLock:
1. Searches for the control slot corresponding to the given handle
2. Verifies the segment is currently pinned and validates reference counts
3. Calls implementation-specific cleanup (dsm_impl_unpin_segment) for non-main-region segments
4. Decrements the reference count and marks the segment as unpinned
5. If the reference count drops to 1 (meaning no active references), initiates segment destruction

The destruction process includes calling dsm_impl_op with DSM_OP_DESTROY and, for main-region segments, returning pages to the FreePageManager.

## Parameters / Member Variables
- `handle`: The dsm_handle identifying the segment to unpin. The segment must have been previously pinned with dsm_pin_segment.

## Dependencies
- Functions called/Symbols referenced:
  - [LWLockAcquire](../L/LWLockAcquire.md)/LWLockRelease (with DynamicSharedMemoryControlLock)
  - [is_main_region_dsm_handle](../i/is_main_region_dsm_handle.md)
  - [dsm_impl_unpin_segment](dsm_impl_unpin_segment.md)
  - [dsm_impl_op](dsm_impl_op.md) (with DSM_OP_DESTROY)
  - [FreePageManagerPut](../F/FreePageManagerPut.md)
  - dsm_handle, dsm_segment, FreePageManager (types)
  - dsm_control (global control structure)
  - INVALID_CONTROL_SLOT constant
- Called from (representative examples):
  - [dsa_release_in_place](dsa_release_in_place.md) (src/backend/utils/mmgr/dsa.c:622)  
  - [destroy_superblock](destroy_superblock.md) (src/backend/utils/mmgr/dsa.c:1877)

## Notes and Other Information
- Must only be called on segments that were previously pinned (will ERROR otherwise)
- Uses dsm_handle instead of dsm_segment to allow unpinning segments not mapped by current process
- Performs extensive validation including reference count checks and pinned status verification
- May trigger immediate segment destruction if this was the final reference keeping it alive
- For main-region segments, returns allocated pages back to the shared free page manager
- Error handling ensures atomic updates and proper cleanup even in failure scenarios
- The reference count semantics: 0=unused slot, 1=no active references, >1=active references

## Simplified Source

```c
void dsm_unpin_segment(dsm_handle handle)
{
    uint32 control_slot = INVALID_CONTROL_SLOT;
    bool destroy = false;
    uint32 i;

    // Find the control slot for the given handle
    LWLockAcquire(DynamicSharedMemoryControlLock, LW_EXCLUSIVE);
    for (i = 0; i < dsm_control->nitems; ++i)
    {
        // Skip unused slots and segments that are concurrently going away
        if (dsm_control->item[i].refcnt <= 1)
            continue;

        // If we've found our handle, we can stop searching
        if (dsm_control->item[i].handle == handle)
        {
            control_slot = i;
            break;
        }
    }

    // Validate that we found the slot and it's properly pinned
    if (control_slot == INVALID_CONTROL_SLOT)
        elog(ERROR, "cannot unpin unknown segment handle");
    if (!dsm_control->item[control_slot].pinned)
        elog(ERROR, "cannot unpin a segment that is not pinned");
    Assert(dsm_control->item[control_slot].refcnt > 1);

    // Allow implementation-specific cleanup
    if (!is_main_region_dsm_handle(handle))
        dsm_impl_unpin_segment(handle,
                               &dsm_control->item[control_slot].impl_private_pm_handle);

    // Decrement reference count and unpin
    if (--dsm_control->item[control_slot].refcnt == 1)
        destroy = true;
    dsm_control->item[control_slot].pinned = false;

    LWLockRelease(DynamicSharedMemoryControlLock);

    // Clean up resources if that was the last reference
    if (destroy)
    {
        void *junk_impl_private = NULL;
        void *junk_mapped_address = NULL;
        Size junk_mapped_size = 0;

        // Destroy the segment
        if (is_main_region_dsm_handle(handle) ||
            dsm_impl_op(DSM_OP_DESTROY, handle, 0, &junk_impl_private,
                        &junk_mapped_address, &junk_mapped_size, WARNING))
        {
            LWLockAcquire(DynamicSharedMemoryControlLock, LW_EXCLUSIVE);
            if (is_main_region_dsm_handle(handle))
                FreePageManagerPut((FreePageManager *) dsm_main_space_begin,
                                   dsm_control->item[control_slot].first_page,
                                   dsm_control->item[control_slot].npages);
            Assert(dsm_control->item[control_slot].handle == handle);
            Assert(dsm_control->item[control_slot].refcnt == 1);
            dsm_control->item[control_slot].refcnt = 0;
            LWLockRelease(DynamicSharedMemoryControlLock);
        }
    }
}
```