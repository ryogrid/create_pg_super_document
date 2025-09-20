# assign_synchronous_commit

## Location
[src/backend/replication/syncrep.c:1121-1138](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/syncrep.c#L1121-L1138)

## Overview
A GUC assign hook function that translates the synchronous_commit configuration parameter value into the appropriate internal synchronous replication wait mode.

## Definition

```c
void
assign_synchronous_commit(int newval, void *extra)
```
## Detailed Description
This function serves as the assignment hook for the synchronous_commit GUC parameter in PostgreSQL's configuration system. It translates the user-facing synchronous_commit setting values into the internal SyncRepWaitMode values that control how the synchronous replication system behaves.

The function maps different levels of synchronous commit requirements to their corresponding internal wait modes:
- Remote write: waits for WAL to be written to standby's OS
- Remote flush: waits for WAL to be flushed to disk on standby  
- Remote apply: waits for WAL to be applied on standby
- All other values (including 'off' and 'local') result in no waiting for standby confirmation

This mapping is crucial for determining how long a transaction will wait for standby server confirmation before being considered committed.

## Parameters / Member Variables
- : Integer value representing the new synchronous_commit setting
- : Additional data pointer (unused in this function)

## Dependencies
- Functions called/Symbols referenced:
  - SYNCHRONOUS_COMMIT_REMOTE_WRITE (constant for remote write mode)
  - SYNCHRONOUS_COMMIT_REMOTE_FLUSH (constant for remote flush mode)  
  - SYNCHRONOUS_COMMIT_REMOTE_APPLY (constant for remote apply mode)
  - SYNC_REP_WAIT_WRITE (internal wait mode for write)
  - SYNC_REP_WAIT_FLUSH (internal wait mode for flush)
  - SYNC_REP_WAIT_APPLY (internal wait mode for apply)
  - SYNC_REP_NO_WAIT (internal mode for no waiting)
- Called from (representative examples):
  - GUC system (referenced in guc_hooks.h)

## Notes and Other Information
- Updates the global SyncRepWaitMode variable which controls synchronous replication behavior
- The default case handles 'off', 'local', and any other values by setting no wait mode
- This function is called whenever the synchronous_commit parameter is changed
- The mapping ensures that the internal replication system uses the correct waiting behavior based on user configuration
- Part of the GUC hook system that translates user-facing configuration into internal system state
- No validation is performed - the GUC system handles value validation before calling this function