# dsm_cleanup_using_control_segment

## Location
[src/backend/storage/ipc/dsm.c:238-319](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/ipc/dsm.c#L238-L319)

## Overview
Cleans up leftover dynamic shared memory segments from a previous postmaster invocation by using an old control segment to locate and destroy orphaned DSM segments.

## Definition
```c
void dsm_cleanup_using_control_segment(dsm_handle old_control_handle)
```

## Detailed Description
This function performs cleanup of dynamic shared memory segments that may have been left behind from a previous PostgreSQL postmaster process. It attempts to attach to an old control segment using the provided handle, validates its contents, and then systematically destroys all DSM segments referenced within that control segment. The function handles cases where the operating system has been rebooted or the segment is corrupted by gracefully falling out. After cleaning up all referenced segments, it also destroys the control segment itself. This cleanup is essential for preventing resource leaks and ensuring that shared memory segments don't accumulate across postmaster restarts.

## Parameters / Member Variables
- `old_control_handle`: Handle to the control segment from a previous postmaster invocation that needs to be cleaned up

## Dependencies
- Functions called/Symbols referenced:
  - [dsm_impl_op](dsm_impl_op.md) (with DSM_OP_ATTACH, DSM_OP_DETACH, DSM_OP_DESTROY)
  - [dsm_control_segment_sane](dsm_control_segment_sane.md)
  - [is_main_region_dsm_handle](../i/is_main_region_dsm_handle.md)
  - elog
- Called from (representative examples):
  - [PGSharedMemoryCreate](../P/PGSharedMemoryCreate.md)

## Notes and Other Information
- Gracefully handles cases where the old control segment no longer exists (e.g., after system reboot)
- Uses dsm_control_segment_sane() to validate the control segment before processing
- Skips cleanup for segments using the main shared memory region (handled differently)
- Logs debugging information about each orphaned segment being cleaned up
- Only processes slots with non-zero reference counts (zero means unused)
- The function is resilient to corruption and safely detaches from invalid segments
- Part of PostgreSQL's shared memory management during startup cleanup phase
- Essential for preventing shared memory leaks across postmaster restarts

## Simplified Source

```c
// Simplified version of dsm_cleanup_using_control_segment
void dsm_cleanup_using_control_segment(dsm_handle old_control_handle) {
    void *mapped_address = NULL;
    void *impl_private = NULL;
    Size mapped_size = 0;
    dsm_control_header *old_control;

    // Step 1: Try to attach to the old control segment
    if (!dsm_impl_op(DSM_OP_ATTACH, old_control_handle, 0, &impl_private,
                     &mapped_address, &mapped_size, DEBUG1)) {
        // Segment no longer exists (e.g., after reboot) - nothing to clean
        return;
    }

    // Step 2: Validate the control segment contents
    old_control = (dsm_control_header *) mapped_address;
    if (!dsm_control_segment_sane(old_control, mapped_size)) {
        // Contents are corrupted - detach and exit
        dsm_impl_op(DSM_OP_DETACH, old_control_handle, 0, &impl_private,
                    &mapped_address, &mapped_size, LOG);
        return;
    }

    // Step 3: Clean up all referenced DSM segments
    uint32 nitems = old_control->nitems;
    for (uint32 i = 0; i < nitems; i++) {
        uint32 refcnt = old_control->item[i].refcnt;
        dsm_handle handle = old_control->item[i].handle;

        // Skip unused slots and main region handles
        if (refcnt == 0 || is_main_region_dsm_handle(handle)) {
            continue;
        }

        // Destroy the orphaned segment
        elog(DEBUG2, "cleaning up orphaned dynamic shared memory with ID %u", handle);
        dsm_impl_op(DSM_OP_DESTROY, handle, 0, NULL, NULL, NULL, LOG);
    }

    // Step 4: Destroy the control segment itself
    elog(DEBUG2, "cleaning up dynamic shared memory control segment with ID %u",
         old_control_handle);
    dsm_impl_op(DSM_OP_DESTROY, old_control_handle, 0, &impl_private,
                &mapped_address, &mapped_size, LOG);
}
```

Key simplifications made:
- Removed duplicate variables for junk operations (simplified to NULL parameters)
- Consolidated variable declarations for clarity
- Added step-by-step comments explaining the main phases
- Simplified error handling while preserving essential logic
- Focused on the core cleanup algorithm flow
- Removed detailed debugging parameter handling for brevity