# dsm_postmaster_shutdown

## Location
src/backend/storage/ipc/dsm.c: 358 - 422

## Overview
A cleanup function called during postmaster shutdown to remove all remaining dynamic shared memory segments and destroy the control segment itself.

## Definition


## Detailed Description
This function performs critical cleanup operations during PostgreSQL postmaster shutdown to ensure proper cleanup of dynamic shared memory resources. It iterates through the DSM control segment to identify and remove all remaining dynamic shared memory segments that may have been left behind by backends. The function is designed to be robust and avoid throwing errors during shutdown, as resource cleanup should not prevent the postmaster from shutting down.

The function first performs sanity checks on the control segment to detect potential corruption that could have occurred if another backend exited uncleanly. If corruption is detected, it logs a warning and exits early to avoid crashes. For valid control segments, it systematically removes each active segment before finally destroying the control segment itself.

## Parameters / Member Variables
- : Exit code passed to the shutdown hook (unused in this function)
- : Datum containing a pointer to the PGShmemHeader structure

## Dependencies
- Functions called/Symbols referenced:
  - dsm_control_segment_sane
  - is_main_region_dsm_handle
  - dsm_impl_op
  - ereport
  - elog
  - DatumGetPointer
- Called from (representative examples):
  - dsm_postmaster_startup (registered as shutdown hook)

## Notes and Other Information
- Designed to be error-tolerant during shutdown - avoids throwing errors that could prevent clean shutdown
- Logs debugging information about segment cleanup operations at DEBUG2 level
- Handles corrupted control segments gracefully by logging warnings and continuing
- Skips main region DSM handles during cleanup as they require special handling
- Updates the shared memory header to clear the dsm_control pointer after cleanup
- Uses LOG error level for dsm_impl_op operations to avoid throwing errors during shutdown