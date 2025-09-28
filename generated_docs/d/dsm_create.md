# dsm_create

## Location
[src/backend/storage/ipc/dsm.c:516-664](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/ipc/dsm.c#L516-L664)

## Overview
Creates a new dynamic shared memory segment with specified size and flags, managing the allocation either from the main shared memory region or by creating a new system-level memory segment.

## Definition

```c
struction, so start at 2 */
			dsm_control->item[i].refcnt = 2;
```
## Detailed Description
The  function is the primary interface for creating new dynamic shared memory (DSM) segments in PostgreSQL. It handles the complete lifecycle of segment creation, including memory allocation, control structure management, and reference counting setup.

The function first attempts to allocate space from the main shared memory region if available (using FreePageManager). If that fails or if the main region isn't available, it creates a new system-level memory segment using platform-specific implementations. The function manages the DSM control segment to track all active segments and ensures proper reference counting to prevent premature destruction.

Key behaviors include:
- Automatic initialization of DSM subsystem if not already done
- Preference for main shared memory region allocation when possible
- Fallback to system-level segment creation with collision-resistant handle generation
- Integration with PostgreSQL's resource management system
- Thread-safe operation using DynamicSharedMemoryControlLock

## Parameters / Member Variables
- : The requested size in bytes for the new DSM segment
- : Control flags, including DSM_CREATE_NULL_IF_MAXSEGMENTS to return NULL instead of erroring when segment limit is reached

## Dependencies
- Functions called/Symbols referenced:
  - [dsm_backend_startup](dsm_backend_startup.md) (initialization)
  - [dsm_create_descriptor](dsm_create_descriptor.md) (descriptor creation)
  - [FreePageManagerGet](../F/FreePageManagerGet.md)/FreePageManagerPut (main region allocation)
  - [make_main_region_dsm_handle](../m/make_main_region_dsm_handle.md) (handle generation for main region)
  - [dsm_impl_op](dsm_impl_op.md) (platform-specific segment operations)
  - [pg_prng_uint32](../p/pg_prng_uint32.md) (random handle generation)
  - [ResourceOwnerForgetDSM](../R/ResourceOwnerForgetDSM.md) (resource management)
- Called from (representative examples):
  - [GetSessionDsmHandle](../G/GetSessionDsmHandle.md) (session management)
  - [InitializeParallelDSM](../I/InitializeParallelDSM.md) (parallel processing)
  - [dsa_create_ext](dsa_create_ext.md) (dynamic shared arrays)
  - [GetNamedDSMSegment](../G/GetNamedDSMSegment.md) (named segment registry)

## Notes and Other Information
- Must be called under postmaster or in single-user mode (safety assertion)
- Uses reference count of 2 initially (count of 1 triggers destruction)
- Integrates with CurrentResourceOwner for automatic cleanup
- Handle generation uses even numbers only for collision avoidance
- Supports both main shared memory region and system-level segment allocation
- Thread-safe through DynamicSharedMemoryControlLock usage
- Returns NULL only when DSM_CREATE_NULL_IF_MAXSEGMENTS flag is set and limit reached

## Simplified Source

```c
// Simplified version of dsm_create
dsm_segment *dsm_create(Size size, int flags) {
    dsm_segment *seg;
    uint32 i, nitems;
    size_t npages = 0, first_page = 0;
    FreePageManager *dsm_main_space_fpm = dsm_main_space_begin;
    bool using_main_dsm_region = false;

    Assert(IsUnderPostmaster || !IsPostmasterEnvironment);

    if (!dsm_init_done)
        dsm_backend_startup();

    // Create segment descriptor
    seg = dsm_create_descriptor();

    // Try to allocate from main shared memory region first
    if (dsm_main_space_fpm) {
        npages = size / FPM_PAGE_SIZE;
        if (size % FPM_PAGE_SIZE > 0)
            ++npages;

        LWLockAcquire(DynamicSharedMemoryControlLock, LW_EXCLUSIVE);
        if (FreePageManagerGet(dsm_main_space_fpm, npages, &first_page)) {
            // Successfully allocated from main region
            seg->mapped_address = (char *) dsm_main_space_begin +
                first_page * FPM_PAGE_SIZE;
            seg->mapped_size = npages * FPM_PAGE_SIZE;
            using_main_dsm_region = true;
        }
    }

    if (!using_main_dsm_region) {
        // Create new system-level segment
        if (dsm_main_space_fpm)
            LWLockRelease(DynamicSharedMemoryControlLock);

        // Generate unique handle and create segment
        for (;;) {
            seg->handle = pg_prng_uint32(&pg_global_prng_state) << 1;  // Even numbers only
            if (seg->handle == DSM_HANDLE_INVALID)
                continue;
            if (dsm_impl_op(DSM_OP_CREATE, seg->handle, size, &seg->impl_private,
                           &seg->mapped_address, &seg->mapped_size, ERROR))
                break;
        }
        LWLockAcquire(DynamicSharedMemoryControlLock, LW_EXCLUSIVE);
    }

    // Find unused control slot
    nitems = dsm_control->nitems;
    for (i = 0; i < nitems; ++i) {
        if (dsm_control->item[i].refcnt == 0) {
            // Set up control slot
            if (using_main_dsm_region) {
                seg->handle = make_main_region_dsm_handle(i);
                dsm_control->item[i].first_page = first_page;
                dsm_control->item[i].npages = npages;
            }
            dsm_control->item[i].handle = seg->handle;
            dsm_control->item[i].refcnt = 2;  // Start at 2, not 1
            dsm_control->item[i].impl_private_pm_handle = NULL;
            dsm_control->item[i].pinned = false;
            seg->control_slot = i;
            LWLockRelease(DynamicSharedMemoryControlLock);
            return seg;
        }
    }

    // Check if we can create a new slot
    if (nitems >= dsm_control->maxitems) {
        // Clean up and handle max segments error
        if (using_main_dsm_region)
            FreePageManagerPut(dsm_main_space_fpm, first_page, npages);
        LWLockRelease(DynamicSharedMemoryControlLock);

        if (!using_main_dsm_region)
            dsm_impl_op(DSM_OP_DESTROY, seg->handle, 0, &seg->impl_private,
                       &seg->mapped_address, &seg->mapped_size, WARNING);

        // Cleanup segment descriptor
        if (seg->resowner != NULL)
            ResourceOwnerForgetDSM(seg->resowner, seg);
        dlist_delete(&seg->node);
        pfree(seg);

        if ((flags & DSM_CREATE_NULL_IF_MAXSEGMENTS) != 0)
            return NULL;
        ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_RESOURCES),
                       errmsg("too many dynamic shared memory segments")));
    }

    // Create new control slot
    if (using_main_dsm_region) {
        seg->handle = make_main_region_dsm_handle(nitems);
        dsm_control->item[nitems].first_page = first_page;
        dsm_control->item[nitems].npages = npages;
    }
    dsm_control->item[nitems].handle = seg->handle;
    dsm_control->item[nitems].refcnt = 2;
    dsm_control->item[nitems].impl_private_pm_handle = NULL;
    dsm_control->item[nitems].pinned = false;
    seg->control_slot = nitems;
    dsm_control->nitems++;
    LWLockRelease(DynamicSharedMemoryControlLock);

    return seg;
}
```

Key simplifications made:
- Consolidated main region vs system-level allocation paths
- Simplified control slot search and allocation logic
- Maintained critical error handling and cleanup paths
- Preserved the two-tier allocation strategy
- Focused on the core create-and-register flow
- Kept essential resource management and reference counting