# pg_promote

## Location
[src/backend/access/transam/xlogfuncs.c:669-714](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xlogfuncs.c#L669-L714)

## Overview
Promotes a standby PostgreSQL server to become a primary server, optionally waiting for the promotion to complete within a specified time limit.

## Definition
```c
Datum pg_promote(PG_FUNCTION_ARGS)
```

## Detailed Description
This function initiates the promotion of a standby server to a primary server. Promotion is the process by which a read-only standby server transitions to become a read-write primary server, typically used in failover scenarios.

The function works by creating a promotion signal file and sending a SIGUSR1 signal to the postmaster process. The postmaster detects this signal and begins the promotion process, which involves:
- Stopping WAL replay
- Writing an end-of-recovery record
- Transitioning to normal read-write operations

The function has two modes of operation:
1. **Asynchronous mode** (wait=false): Returns immediately after initiating promotion
2. **Synchronous mode** (wait=true): Waits up to the specified number of seconds for promotion to complete

If waiting is requested, the function polls the recovery status every 100ms for the specified duration. If promotion doesn't complete within the timeout, it returns false and issues a warning.

## Parameters / Member Variables
-  (bool): Whether to wait for promotion to complete before returning
-  (int32): Maximum time in seconds to wait for promotion (must be positive)

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_BOOL, PG_GETARG_INT32 (parameter extraction macros)
  - [RecoveryInProgress](../R/RecoveryInProgress.md) (checks if server is in recovery mode)
  - [AllocateFile](../A/AllocateFile.md), FreeFile (file operations for promotion signal)
  - kill (sends signal to postmaster process)
  - unlink (removes promotion signal file on error)
  - [ResetLatch](../R/ResetLatch.md), WaitLatch (waiting mechanism when wait=true)
  - ereport (error reporting)
  - PROMOTE_SIGNAL_FILE (constant for signal file path)
- Called from (representative examples):
  - No direct callers found in the codebase (likely called via SQL function interface)

## Notes and Other Information
- Can only be executed when the server is in recovery mode (standby server)
- Returns an error if called on a server that's not in recovery
- The wait_seconds parameter must be positive (> 0)
- Uses a polling interval of 100ms (WAITS_PER_SECOND = 10) when waiting
- Creates a promotion signal file that the postmaster monitors
- Returns true if promotion succeeds (or is initiated when wait=false)
- Returns false if promotion doesn't complete within the specified timeout
- Essential function for PostgreSQL high-availability and failover scenarios
- Defined in src/backend/access/transam/xlogfuncs.c:669-749

## Simplified Source

```c
Datum
pg_promote(PG_FUNCTION_ARGS)
{
    bool wait = PG_GETARG_BOOL(0);
    int wait_seconds = PG_GETARG_INT32(1);
    FILE *promote_file;

    // Must be in recovery mode to promote
    if (!RecoveryInProgress())
        ereport(ERROR, "recovery is not in progress");

    // Validate wait timeout
    if (wait_seconds <= 0)
        ereport(ERROR, "wait_seconds must be positive");

    // Create promotion signal file
    promote_file = AllocateFile(PROMOTE_SIGNAL_FILE, "w");
    if (!promote_file)
        ereport(ERROR, "could not create promote signal file");

    if (FreeFile(promote_file))
        ereport(ERROR, "could not write promote signal file");

    // Signal postmaster to start promotion
    if (kill(PostmasterPid, SIGUSR1) != 0) {
        unlink(PROMOTE_SIGNAL_FILE);
        ereport(ERROR, "failed to send signal to postmaster");
    }

    // Return immediately if not waiting
    if (!wait)
        PG_RETURN_BOOL(true);

    // Poll for promotion completion up to wait_seconds
    for (int i = 0; i < wait_seconds * 10; i++) {
        if (!RecoveryInProgress())
            PG_RETURN_BOOL(true);  // Promotion completed

        // Wait 100ms before checking again
        WaitLatch(MyLatch, WL_LATCH_SET | WL_TIMEOUT, 100,
                  WAIT_EVENT_PROMOTE);
        ResetLatch(MyLatch);
    }

    // Promotion didn't complete in time
    ereport(WARNING, "promotion did not complete within %d seconds",
            wait_seconds);
    PG_RETURN_BOOL(false);
}
```