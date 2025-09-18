# pgarch_MainLoop

## Location
[src/backend/postmaster/pgarch.c:310-379](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/postmaster/pgarch.c#L310-L379)

## Overview
The main execution loop for the PostgreSQL archiver process that continuously monitors for WAL files to archive, handles shutdown signals, and manages periodic wakeups.

## Definition
static void pgarch_MainLoop(void)

## Detailed Description
pgarch_MainLoop implements the core control loop for the PostgreSQL archiver background process. The function runs continuously until a shutdown condition is met, performing the following key operations:

1. **Signal Processing**: Checks for SIGUSR2 (graceful shutdown) and SIGTERM (immediate shutdown) signals
2. **Archival Operations**: Calls pgarch_ArchiverCopyLoop() to perform the actual WAL file archiving work
3. **Periodic Wakeups**: Uses WaitLatch() with a 60-second timeout to ensure proactive archiving even without signals
4. **Graceful Shutdown**: Handles shutdown requests by performing one final archiving cycle before exiting
5. **Emergency Exit**: Implements a 60-second timeout after SIGTERM to prevent indefinite hanging

The loop continues until either the postmaster dies or a graceful shutdown is requested via SIGUSR2. The archiver is designed to be proactive, waking up periodically to check for new WAL files even when no explicit signals are received.

## Parameters / Member Variables
- No parameters (void function)
- Local variables:
  - : Boolean flag indicating when to exit the main loop

## Dependencies
- Functions called/Symbols referenced:
  - [ResetLatch](../R/ResetLatch.md) (clear the latch state)
  - [HandlePgArchInterrupts](../H/HandlePgArchInterrupts.md) (process barrier events and config updates)
  - [pgarch_ArchiverCopyLoop](pgarch_ArchiverCopyLoop.md) (perform actual archiving work)
  - [WaitLatch](../W/WaitLatch.md) (wait for signals or timeout)
  - WL_LATCH_SET, WL_TIMEOUT, WL_POSTMASTER_DEATH (wait event flags)
  - PGARCH_AUTOWAKE_INTERVAL (60-second timeout constant)
- Called from (representative examples):
  - [PgArchiverMain](../P/PgArchiverMain.md) (main archiver entry point)

## Notes and Other Information
- This is a static function internal to the pgarch.c module
- The function implements a defensive approach to ensure archiving continues even during signal handling edge cases
- Uses a 60-second emergency timeout after SIGTERM to prevent the archiver from hanging indefinitely
- The loop structure ensures at least one final archiving cycle is performed during graceful shutdown
- Relies on WaitLatch for efficient event-driven operation with periodic fallback polling