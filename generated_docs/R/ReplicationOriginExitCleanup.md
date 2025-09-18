# ReplicationOriginExitCleanup

## Location
[src/backend/replication/logical/origin.c:1055-1096](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/origin.c#L1055-L1096)

## Overview
Cleans up a configured session replication origin during process exit, releasing any acquired replication origin and notifying waiting processes.

## Definition


## Detailed Description
ReplicationOriginExitCleanup is a process exit callback function that ensures proper cleanup of replication origin sessions when a PostgreSQL backend process terminates. It checks if the current process has acquired a session replication origin, and if so, releases it by clearing the acquired_by field and resetting the session state. The function also broadcasts a condition variable to wake up any processes that might be waiting to acquire the same replication origin, ensuring no deadlocks or resource leaks occur during process termination.

## Parameters / Member Variables
- : int representing the exit code (standard exit callback parameter)
- : Datum containing additional arguments (standard exit callback parameter, unused here)

## Dependencies
- Functions called/Symbols referenced:
  - LWLockAcquire/LWLockRelease
  - ConditionVariableBroadcast
  - ConditionVariable (struct)
  - ReplicationOriginLock
  - LW_EXCLUSIVE
  - MyProcPid
- Called from (representative examples):
  - Process exit callback system (registered in replorigin_session_setup)

## Notes and Other Information
- This is a static function, only accessible within the origin.c file
- Registered as an exit callback in replorigin_session_setup to ensure cleanup happens automatically
- Uses exclusive locking to prevent race conditions during cleanup
- The condition variable broadcast ensures that other processes waiting for the origin are notified
- Critical for preventing resource leaks and ensuring proper cleanup of replication origin sessions
- Only performs cleanup if the current process actually acquired the session replication state
- Part of the broader session management infrastructure for logical replication