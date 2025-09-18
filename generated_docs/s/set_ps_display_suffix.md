# set_ps_display_suffix

## Location
[src/backend/utils/misc/ps_status.c:369-420](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/misc/ps_status.c#L369-L420)

## Overview
Appends a suffix string to the current process title with a space separator, allowing PostgreSQL processes to show additional status information in their process names.

## Definition


## Detailed Description
This function modifies the current process title by appending a suffix string, separated by a space from the existing title. It handles buffer management carefully to ensure the suffix fits within the available space:

- If a suffix already exists, it overwrites the previous one
- Maintains a record of the original title length (ps_buffer_nosuffix_len) for suffix removal
- When insufficient space exists, truncates the suffix to fit within the buffer limits
- Always ensures null-termination and calls flush_ps_display() to update the actual process title

The function is commonly used by PostgreSQL processes to indicate their current activity or state, such as waiting for locks, synchronous replication, or performing maintenance operations.

## Parameters / Member Variables
- : A null-terminated string to append to the process title

## Dependencies
- Functions called/Symbols referenced:
  - [update_ps_display_precheck](../u/update_ps_display_precheck.md) (prerequisite validation)
  - flush_ps_display (applies the title change to the system)
  - strlen, memcpy (string manipulation functions)
  - Assert (debugging assertion macro)
- Called from (representative examples):
  - [SyncRepWaitForLSN](../S/SyncRepWaitForLSN.md) (src/backend/replication/syncrep.c:262)
  - LockBufferForCleanup (src/backend/storage/buffer/bufmgr.c:5281)
  - [ResolveRecoveryConflictWithVirtualXIDs](../R/ResolveRecoveryConflictWithVirtualXIDs.md) (src/backend/storage/ipc/standby.c:422)
  - [WaitOnLock](../W/WaitOnLock.md) (src/backend/storage/lmgr/lock.c:1827)

## Notes and Other Information
- Protected by PS_USE_NONE compilation flag - becomes a no-op when process status display is disabled
- Handles buffer overflow gracefully by truncating the suffix rather than failing
- Maintains internal state variables (ps_buffer_cur_len, ps_buffer_nosuffix_len) for proper suffix management
- The space separator is added automatically - callers should not include leading spaces in the suffix
- Used extensively throughout PostgreSQL for showing wait states, replication status, and other transient process activities