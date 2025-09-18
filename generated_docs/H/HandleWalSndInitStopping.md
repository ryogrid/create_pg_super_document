# HandleWalSndInitStopping

## Location
src/backend/replication/walsender.c: 3602 - 3623

## Overview
HandleWalSndInitStopping handles the PROCSIG_WALSND_INIT_STOPPING signal to initiate graceful shutdown of a WAL sender process, with different behavior depending on whether replication is currently active.

## Definition
```c
void HandleWalSndInitStopping(void)
```

## Detailed Description
This function is a signal handler for the PROCSIG_WALSND_INIT_STOPPING signal. It implements a two-phase shutdown strategy: if replication has not yet started, it immediately terminates the process using SIGTERM. However, if replication is active, it sets the got_STOPPING flag to true, which allows the main WAL sender loop to complete any outstanding WAL transmission and wait for acknowledgment from the standby before gracefully exiting. This ensures data consistency during shutdown.

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - kill (system call)
  - Assert (macro)
  - am_walsender (global variable)
  - replication_active (global variable)
  - MyProcPid (global variable)
  - got_STOPPING (global flag)
- Called from (representative examples):
  - [procsignal_sigusr1_handler](../p/procsignal_sigusr1_handler.md)
  - [CRSSnapshotAction](../C/CRSSnapshotAction.md)

## Notes and Other Information
- This function must only be called from within a WAL sender process (asserted by am_walsender check)
- The graceful shutdown mechanism ensures that any pending WAL data is properly transmitted before termination
- Part of the PostgreSQL process signaling infrastructure for coordinated shutdown
- The got_STOPPING flag is checked by the main WAL sender loop to initiate graceful shutdown procedures