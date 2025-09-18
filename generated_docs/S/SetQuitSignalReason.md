# SetQuitSignalReason

## Location
[src/backend/storage/ipc/pmsignal.c:218-228](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/ipc/pmsignal.c#L218-L228)

## Overview
Broadcasts the reason for a system shutdown by setting a shared memory field that can be accessed by all PostgreSQL backend processes.

## Definition


## Detailed Description
This function sets the shutdown reason in shared memory before the postmaster sends SIGQUIT signals to child processes. It stores the reason in the PMSignalState structure, allowing child processes to understand why they are being terminated. The function is typically called by the postmaster during controlled shutdown scenarios to provide context about whether the shutdown is due to a crash, an immediate stop command, or other reasons. In crash-and-restart scenarios, the reason field is automatically cleared as part of shared memory reconstruction, so explicit clearing by the postmaster is not required.

## Parameters / Member Variables
- : A QuitSignalReason enum value indicating why the system is shutting down
  - : postmaster hasn't sent SIGQUIT
  - : some other backend crashed
  - : immediate stop was commanded

## Dependencies
- Functions called/Symbols referenced:
  - PMSignalState (global shared memory structure)
  - QuitSignalReason (enum type)
- Called from (representative examples):
  - [process_pm_shutdown_request](../p/process_pm_shutdown_request.md) (src/backend/postmaster/postmaster.c:2327)
  - [HandleChildCrash](../H/HandleChildCrash.md) (src/backend/postmaster/postmaster.c:2896)

## Notes and Other Information
- Must be called before sending SIGQUIT to children to ensure they have context for the shutdown
- The reason field is automatically cleared during crash recovery when shared memory is rebuilt
- Located in src/backend/storage/ipc/pmsignal.c:218-228
- Part of the postmaster signaling mechanism for coordinated shutdown procedures