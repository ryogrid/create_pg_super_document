# dsm_postmaster_shutdown

## Location
[src/backend/storage/ipc/dsm.c:358-422](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/ipc/dsm.c#L358-L422)

## Overview
A cleanup function called during postmaster shutdown to remove all remaining dynamic shared memory segments and destroy the control segment itself.

## Definition

```c
static void
dsm_postmaster_shutdown(int code, Datum arg)
```
## Detailed Description
This function performs critical cleanup operations during PostgreSQL postmaster shutdown to ensure proper cleanup of dynamic shared memory resources. It iterates through the DSM control segment to identify and remove all remaining dynamic shared memory segments that may have been left behind by backends. The function is designed to be robust and avoid throwing errors during shutdown, as resource cleanup should not prevent the postmaster from shutting down.

The function first performs sanity checks on the control segment to detect potential corruption that could have occurred if another backend exited uncleanly. If corruption is detected, it logs a warning and exits early to avoid crashes. For valid control segments, it systematically removes each active segment before finally destroying the control segment itself.

## Parameters / Member Variables
- `code`: Exit code passed to the shutdown hook (unused in this function)
- `arg`: Datum containing a pointer to the PGShmemHeader structure
## Dependencies
- Functions called/Symbols referenced:
  - [dsm_control_segment_sane](dsm_control_segment_sane.md)
  - [is_main_region_dsm_handle](../i/is_main_region_dsm_handle.md)
  - [dsm_impl_op](dsm_impl_op.md)
  - ereport
  - elog
  - [DatumGetPointer](../D/DatumGetPointer.md)
- Called from (representative examples):
  - [dsm_postmaster_startup](dsm_postmaster_startup.md) (registered as shutdown hook)

## Notes and Other Information
- Designed to be error-tolerant during shutdown - avoids throwing errors that could prevent clean shutdown
- Logs debugging information about segment cleanup operations at DEBUG2 level
- Handles corrupted control segments gracefully by logging warnings and continuing
- Skips main region DSM handles during cleanup as they require special handling
- Updates the shared memory header to clear the dsm_control pointer after cleanup
- Uses LOG error level for dsm_impl_op operations to avoid throwing errors during shutdown

## Simplified Source

```c
// Simplified version of dsm_postmaster_shutdown
static void dsm_postmaster_shutdown(int code, Datum arg) {
    // Get the shared memory header from the argument
    PGShmemHeader *shim = (PGShmemHeader *) DatumGetPointer(arg);

    // Check if the control segment is corrupted
    uint32 nitems = dsm_control->nitems;
    if (!dsm_control_segment_sane(dsm_control, dsm_control_mapped_size)) {
        // Log corruption warning and exit early
        ereport(LOG, (errmsg("dynamic shared memory control segment is corrupt")));
        return;
    }

    // Remove all remaining DSM segments
    for (uint32 i = 0; i < nitems; ++i) {
        // Skip unused slots (refcnt == 0)
        if (dsm_control->item[i].refcnt == 0)
            continue;

        dsm_handle handle = dsm_control->item[i].handle;

        // Skip main region handles (special case)
        if (is_main_region_dsm_handle(handle))
            continue;

        // Log cleanup operation
        elog(DEBUG2, "cleaning up orphaned dynamic shared memory with ID %u", handle);

        // Destroy the segment
        dsm_impl_op(DSM_OP_DESTROY, handle, 0, &junk_impl_private,
                    &junk_mapped_address, &junk_mapped_size, LOG);
    }

    // Remove the control segment itself
    elog(DEBUG2, "cleaning up dynamic shared memory control segment with ID %u",
         dsm_control_handle);

    void *dsm_control_address = dsm_control;
    dsm_impl_op(DSM_OP_DESTROY, dsm_control_handle, 0,
                &dsm_control_impl_private, &dsm_control_address,
                &dsm_control_mapped_size, LOG);

    // Clear the control pointer in shared memory header
    dsm_control = dsm_control_address;
    shim->dsm_control = 0;
}
```

Key simplifications made:
- Removed variable declarations for unused junk variables (declared inline where needed)
- Consolidated variable declarations with initialization where possible
- Added descriptive comments for each major section
- Simplified the control flow by grouping related operations
- Focused on the main execution path while preserving all essential logic
- Maintained error handling and logging as they are critical for debugging