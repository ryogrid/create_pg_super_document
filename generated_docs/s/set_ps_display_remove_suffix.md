# set_ps_display_remove_suffix

## Location
src/backend/utils/misc/ps_status.c: 421 - 450

## Overview
Removes the suffix that was previously added to the process title by set_ps_display_suffix, restoring the process title to its original state.

## Definition


## Detailed Description
This function removes a previously added suffix from the process title, effectively restoring the title to the state it was in before set_ps_display_suffix was called. The implementation:

- Validates that process title updates are enabled and appropriate using update_ps_display_precheck()
- Checks if a suffix was previously added (ps_buffer_nosuffix_len > 0)
- Truncates the buffer at the original length by null-terminating at ps_buffer_nosuffix_len
- Resets the current buffer length and suffix tracking variables
- Updates the actual process title via flush_ps_display()

This function is typically called when a PostgreSQL process finishes a temporary activity that required showing additional status information, such as completing a wait operation or finishing a maintenance task.

## Parameters / Member Variables
- None (void function)

## Dependencies
- Functions called/Symbols referenced:
  - update_ps_display_precheck (prerequisite validation)
  - flush_ps_display (applies the title change to the system)
  - strlen (string length calculation for assertion)
  - Assert (debugging assertion macro)
- Called from (representative examples):
  - SyncRepWaitForLSN (src/backend/replication/syncrep.c:362)
  - LockBufferForCleanup (src/backend/storage/buffer/bufmgr.c:5257)
  - ResolveRecoveryConflictWithVirtualXIDs (src/backend/storage/ipc/standby.c:453)
  - WaitOnLock (src/backend/storage/lmgr/lock.c:1881, 1891)

## Notes and Other Information
- Protected by PS_USE_NONE compilation flag - becomes a no-op when process status display is disabled
- Safe to call even when no suffix exists (early return if ps_buffer_nosuffix_len == 0)
- Paired with set_ps_display_suffix for managing temporary process title modifications
- Resets ps_buffer_nosuffix_len to 0, indicating no suffix is currently active
- Commonly used in cleanup code paths and exception handlers to ensure process titles are properly restored