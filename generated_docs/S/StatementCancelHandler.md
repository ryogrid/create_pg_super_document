# StatementCancelHandler

## Location
[src/backend/tcop/postgres.c:3029-3045](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tcop/postgres.c#L3029-L3045)

## Overview
StatementCancelHandler is a signal handler function that processes query-cancel signals from the postmaster to abort the current transaction at the soonest convenient time.

## Definition
```c
void StatementCancelHandler(SIGNAL_ARGS)
```

## Detailed Description
StatementCancelHandler serves as the signal handler for query cancellation requests in PostgreSQL. When the postmaster needs to cancel a running query (typically due to a client request or administrative action), it sends a signal that is caught by this handler. The function sets global flags to indicate that an interrupt is pending and that query cancellation has been requested, allowing the system to gracefully abort the current transaction when it reaches a safe interruption point.

The handler includes a safety check to avoid interfering with the process exit sequence. If the backend is already in the process of exiting (proc_exit_inprogress is true), the handler avoids setting the interrupt flags to prevent complications during shutdown.

## Parameters / Member Variables
- `SIGNAL_ARGS`: Standard signal handler arguments (typically signal number and signal info)

## Dependencies
- Functions called/Symbols referenced:
  - [SetLatch](SetLatch.md) (to wake up anything waiting on the process latch)
  - SIGNAL_ARGS (macro for signal handler parameters)
- Global variables used:
  - proc_exit_inprogress (checked to avoid interference with exit process)
  - InterruptPending (set to true to indicate interrupt is pending)
  - QueryCancelPending (set to true to indicate query cancellation is requested)
  - MyLatch (used with SetLatch to wake waiting processes)
- Called from (representative examples):
  - [PostgresMain](../P/PostgresMain.md) (main backend process)
  - [AutoVacWorkerMain](../A/AutoVacWorkerMain.md) (autovacuum worker processes)
  - [BackgroundWorkerMain](../B/BackgroundWorkerMain.md) (background worker processes)
  - [WalSndSignals](../W/WalSndSignals.md) (WAL sender processes)

## Notes and Other Information
- This is a signal handler function and must be async-signal-safe
- The handler uses a "don't joggle the elbow" approach during process exit to avoid race conditions
- The SetLatch call ensures that any process waiting on the latch will be awakened to check for the cancellation request
- [Query](../Q/Query.md) cancellation is implemented as a cooperative mechanism - the actual cancellation occurs when the backend checks the QueryCancelPending flag at safe interruption points

## Simplified Source

```c
// Simplified version of StatementCancelHandler
void StatementCancelHandler(SIGNAL_ARGS) {
    // Safety check: Don't interfere with process exit sequence
    if (!proc_exit_inprogress) {
        // Set global flags to request query cancellation
        InterruptPending = true;
        QueryCancelPending = true;
    }

    // Wake up any processes waiting on our latch
    SetLatch(MyLatch);
}
```

Key simplifications made:
- Added clear explanatory comments for each logical step
- Maintained the essential safety check and flag-setting logic
- Preserved the latch signaling mechanism
- Focused on the core cooperative cancellation pattern