# dsm_attach

## Location
[src/backend/storage/ipc/dsm.c:665-756](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/ipc/dsm.c#L665-L756)

## Overview
Attaches to an existing dynamic shared memory segment using its handle, incrementing the reference count and mapping the segment into the current process's address space.

## Definition

```c
dsm_segment *
dsm_attach(dsm_handle h)
```
## Detailed Description
The  function provides the mechanism for attaching to an existing DSM segment that was previously created by another process. It performs several critical operations to ensure safe and correct attachment:

1. **Duplicate attachment prevention**: Checks if the segment is already attached to the current process to prevent multiple attachments
2. **Handle lookup**: Searches the DSM control segment to find an active slot with the matching handle
3. **Reference counting**: Increments the segment's reference count to prevent premature destruction
4. **Memory mapping**: Maps the segment into the current process's address space, either from the main shared memory region or via platform-specific operations

The function handles both main region segments (allocated from PostgreSQL's main shared memory) and system-level segments (created via OS-specific mechanisms). It includes robust error handling for cases where the segment may have been destroyed between the time the handle was obtained and the attachment attempt.

## Parameters / Member Variables
- : The DSM handle identifying the segment to attach to, typically obtained from another process or a persistent store

## Dependencies
- Functions called/Symbols referenced:
  - [dsm_backend_startup](dsm_backend_startup.md) (initialization if needed)
  - [dsm_create_descriptor](dsm_create_descriptor.md) (creates local segment descriptor)
  - dlist_foreach/dlist_container (iterates through attached segments)
  - [is_main_region_dsm_handle](../i/is_main_region_dsm_handle.md) (checks if handle is for main region)
  - [dsm_impl_op](dsm_impl_op.md) (platform-specific attachment operations)
  - [dsm_detach](dsm_detach.md) (cleanup on failure)
- Called from (representative examples):
  - [AttachSession](../A/AttachSession.md) (session management)
  - [ParallelWorkerMain](../P/ParallelWorkerMain.md) (parallel query workers)
  - [dsa_attach](dsa_attach.md) (dynamic shared arrays)
  - [GetNamedDSMSegment](../G/GetNamedDSMSegment.md) (named segment registry)

## Notes and Other Information
- Only safe to call under postmaster (assertion enforced)
- Returns NULL if segment not found or already destroyed
- Prevents duplicate attachments to the same segment within a process
- Automatically integrates with CurrentResourceOwner for cleanup tracking
- Reference count must be > 1 for valid attachment (count of 1 indicates pending destruction)
- Handles both main region and system-level segment types transparently
- Thread-safe operation using DynamicSharedMemoryControlLock
- Caller should use dsm_find_mapping() first to check for existing attachments

## Simplified Source

```c
// Simplified version of dsm_attach
dsm_segment *dsm_attach(dsm_handle h) {
    dsm_segment *seg;
    dlist_iter iter;
    uint32 i, nitems;

    Assert(IsUnderPostmaster);

    if (!dsm_init_done)
        dsm_backend_startup();

    // Check for duplicate attachment
    dlist_foreach(iter, &dsm_segment_list) {
        seg = dlist_container(dsm_segment, node, iter.cur);
        if (seg->handle == h)
            elog(ERROR, "can't attach the same segment more than once");
    }

    // Create segment descriptor
    seg = dsm_create_descriptor();
    seg->handle = h;

    // Find and increment reference count in control segment
    LWLockAcquire(DynamicSharedMemoryControlLock, LW_EXCLUSIVE);
    nitems = dsm_control->nitems;
    for (i = 0; i < nitems; ++i) {
        // Skip unused or dying segments
        if (dsm_control->item[i].refcnt <= 1)
            continue;

        // Check for handle match
        if (dsm_control->item[i].handle != seg->handle)
            continue;

        // Found matching segment - increment reference count
        dsm_control->item[i].refcnt++;
        seg->control_slot = i;

        // Set up mapping for main region segments
        if (is_main_region_dsm_handle(seg->handle)) {
            seg->mapped_address = (char *) dsm_main_space_begin +
                dsm_control->item[i].first_page * FPM_PAGE_SIZE;
            seg->mapped_size = dsm_control->item[i].npages * FPM_PAGE_SIZE;
        }
        break;
    }
    LWLockRelease(DynamicSharedMemoryControlLock);

    // Handle case where segment wasn't found
    if (seg->control_slot == INVALID_CONTROL_SLOT) {
        dsm_detach(seg);
        return NULL;
    }

    // Map non-main region segments
    if (!is_main_region_dsm_handle(seg->handle))
        dsm_impl_op(DSM_OP_ATTACH, seg->handle, 0, &seg->impl_private,
                    &seg->mapped_address, &seg->mapped_size, ERROR);

    return seg;
}
```

Key simplifications made:
- Consolidated duplicate attachment check logic
- Simplified control segment search loop with clear comments
- Combined reference counting and mapping setup
- Maintained critical error handling paths
- Preserved the two-tier segment mapping strategy (main region vs system-level)
- Focused on the core attach logic flow