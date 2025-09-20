# StartupStatusEnum

## Location
[src/backend/postmaster/postmaster.c:259-263](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/postmaster/postmaster.c#L259-L263)

## Overview
StartupStatusEnum is an enumeration that tracks the current state of the startup process in PostgreSQL's postmaster.

## Definition

```c
typedef enum
{
	STARTUP_NOT_RUNNING,
	STARTUP_RUNNING,
	STARTUP_SIGNALED,			/* we sent it a SIGQUIT or SIGKILL */
	STARTUP_CRASHED,
} StartupStatusEnum;
```
## Detailed Description
StartupStatusEnum defines the possible states of the PostgreSQL startup process, which is responsible for crash recovery and bringing the database to a consistent state during server startup. The postmaster uses this enum to track the startup process lifecycle and make appropriate decisions during database initialization, recovery, and shutdown procedures.

The startup process is a critical component that runs WAL (Write-Ahead Log) recovery, ensuring the database reaches a consistent state before allowing normal operations to begin. The status tracking helps the postmaster coordinate startup completion and handle error conditions.

## Parameters / Member Variables
- : The startup process is not currently running (initial state or after completion/termination)
- : The startup process is actively running and performing recovery operations
- : The startup process has been sent a termination signal (SIGQUIT or SIGKILL) by the postmaster
- : The startup process has crashed or terminated unexpectedly

## Dependencies
- Functions called/Symbols referenced:
  - STARTUP_NOT_RUNNING (default initialization value)
- Called from (representative examples):
  - Various postmaster functions that manage startup process lifecycle
  - Signal handlers that track startup process state changes
  - Recovery and initialization routines

## Notes and Other Information
- The global variable StartupStatus is initialized to STARTUP_NOT_RUNNING
- State transitions typically follow: NOT_RUNNING → RUNNING → (SIGNALED or CRASHED or NOT_RUNNING)
- The STARTUP_SIGNALED state helps distinguish between normal termination and forced termination
- Critical for crash recovery coordination and ensuring database consistency during startup
- Used by the postmaster to determine when it's safe to accept client connections