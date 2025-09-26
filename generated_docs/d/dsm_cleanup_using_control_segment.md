# dsm_cleanup_using_control_segment

## Location
src/backend/storage/ipc/dsm.c: 238 - 319

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
  - dsm_impl_op (with DSM_OP_ATTACH, DSM_OP_DETACH, DSM_OP_DESTROY)
  - dsm_control_segment_sane
  - is_main_region_dsm_handle
  - elog
- Called from (representative examples):
  - PGSharedMemoryCreate

## Notes and Other Information
- Gracefully handles cases where the old control segment no longer exists (e.g., after system reboot)
- Uses dsm_control_segment_sane() to validate the control segment before processing
- Skips cleanup for segments using the main shared memory region (handled differently)
- Logs debugging information about each orphaned segment being cleaned up
- Only processes slots with non-zero reference counts (zero means unused)
- The function is resilient to corruption and safely detaches from invalid segments
- Part of PostgreSQL's shared memory management during startup cleanup phase
- Essential for preventing shared memory leaks across postmaster restarts