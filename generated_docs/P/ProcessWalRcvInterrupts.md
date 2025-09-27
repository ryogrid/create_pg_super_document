# ProcessWalRcvInterrupts

## Location
[src/backend/replication/walreceiver.c:162-182](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/walreceiver.c#L162-L182)

## Overview
ProcessWalRcvInterrupts handles interrupts for the WAL receiver process, specifically processing shutdown requests that arrive via SIGTERM signals.

## Definition
void ProcessWalRcvInterrupts(void)

## Detailed Description
This function processes any interrupts that the WAL receiver process may have received and should be called whenever the process's latch has become set. The primary purpose is to handle SIGTERM signals safely without interrupting critical operations.

The function uses a two-phase interrupt handling approach: when SIGTERM arrives, the signal handler sets a flag variable (ShutdownRequestPending) and the process latch, rather than calling exit() directly. This prevents interruption during critical operations like holding spinlocks. The function checks this flag and terminates the process gracefully if a shutdown has been requested.

The function also calls CHECK_FOR_INTERRUPTS() to ensure proper signal reception on Windows platforms and to process any barrier events that may be pending.

## Parameters / Member Variables
(This function takes no parameters)

## Dependencies
- Functions called/Symbols referenced:
  - CHECK_FOR_INTERRUPTS
  - ShutdownRequestPending (global variable)
  - ereport
  - FATAL
  - [errcode](../e/errcode.md)
  - ERRCODE_ADMIN_SHUTDOWN
  - [errmsg](../e/errmsg.md)

- Called from (representative examples):
  - [libpqrcv_connect](../l/libpqrcv_connect.md)
  - [libpqrcv_PQgetResult](../l/libpqrcv_PQgetResult.md)
  - [libpqrcv_processTuples](../l/libpqrcv_processTuples.md)
  - [WalReceiverMain](../W/WalReceiverMain.md)
  - [WalRcvWaitForStartPosition](../W/WalRcvWaitForStartPosition.md)
  - [walrcv_clear_result](../w/walrcv_clear_result.md)

## Notes and Other Information
- The function is designed to be called from any location where the WAL receiver process might block for extended periods
- Critical for safe shutdown handling in replication scenarios
- Part of PostgreSQL's streaming replication infrastructure
- The latch-based approach ensures that long-running operations can be interrupted safely

## Simplified Source

```c
// Simplified version of ProcessWalRcvInterrupts
void ProcessWalRcvInterrupts(void) {
    // Step 1: Handle platform-specific signals and barrier events
    CHECK_FOR_INTERRUPTS();

    // Step 2: Check if shutdown was requested via SIGTERM
    if (ShutdownRequestPending) {
        // Terminate gracefully with appropriate error message
        ereport(FATAL,
                (errcode(ERRCODE_ADMIN_SHUTDOWN),
                 errmsg("terminating walreceiver process due to administrator command")));
    }
}
```

Key simplifications made:
- Removed detailed comments for clarity while preserving essential logic
- Focused on the two-step interrupt handling process
- Maintained the core safety mechanism for graceful shutdown
- Preserved the essential function structure and error reporting