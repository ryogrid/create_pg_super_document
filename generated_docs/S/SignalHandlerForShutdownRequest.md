# SignalHandlerForShutdownRequest

## Location
[src/backend/postmaster/interrupt.c:105-109](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/postmaster/interrupt.c#L105-L109)

## Overview
SignalHandlerForShutdownRequest is a signal handler designed to trigger graceful shutdown of long-running background processes in PostgreSQL, typically in response to SIGTERM or other termination signals.

## Definition
```c
void SignalHandlerForShutdownRequest(SIGNAL_ARGS)
```

## Detailed Description
This function serves as a signal handler for requesting graceful shutdown of PostgreSQL background processes. When invoked by termination signals, it sets the global ShutdownRequestPending flag to true and wakes up the process by setting its latch. The actual shutdown processing is deferred to be handled later in the main loop, either through explicit checks of the ShutdownRequestPending flag or by calling HandleMainLoopInterrupts.

Unlike SignalHandlerForCrashExit, this handler is designed for clean, graceful shutdowns where the process has time to complete current operations and perform necessary cleanup before terminating. Different PostgreSQL background processes may use this handler with different signals based on their specific requirements.

## Parameters / Member Variables
- Uses SIGNAL_ARGS macro which typically expands to signal handler parameters (signal number, etc.)

## Dependencies
- Functions called/Symbols referenced:
  - SIGNAL_ARGS (signal handler parameter macro)
  - [SetLatch](SetLatch.md) (to wake up the process)
- Called from (representative examples):
  - [BackgroundWriterMain](../B/BackgroundWriterMain.md) (src/backend/postmaster/bgwriter.c:104)
  - [CheckpointerMain](../C/CheckpointerMain.md) (src/backend/postmaster/checkpointer.c:203)
  - [PgArchiverMain](../P/PgArchiverMain.md) (src/backend/postmaster/pgarch.c:230)
  - [WalSummarizerMain](../W/WalSummarizerMain.md) (src/backend/postmaster/walsummarizer.c:248-249)
  - [WalWriterMain](../W/WalWriterMain.md) (src/backend/postmaster/walwriter.c:108-109)
  - [ParallelApplyWorkerMain](../P/ParallelApplyWorkerMain.md) (src/backend/replication/logical/applyparallelworker.c:874)
  - [ReplSlotSyncWorkerMain](../R/ReplSlotSyncWorkerMain.md) (src/backend/replication/logical/slotsync.c:1393)
  - [WalReceiverMain](../W/WalReceiverMain.md) (src/backend/replication/walreceiver.c:282)

## Notes and Other Information
- This signal handler is declared in src/include/postmaster/interrupt.h
- Typically used for SIGTERM signals, but different processes may use other signals (checkpointer uses SIGUSR2, WAL writer and logical replication parallel apply worker use SIGINT or SIGTERM)
- The handler follows safe signal handling practices by doing minimal work (just setting flags and latches)
- The ShutdownRequestPending flag it sets is typically checked by HandleMainLoopInterrupts or similar interrupt handling functions
- The SetLatch call ensures that processes waiting on their latch will wake up to process the pending shutdown request
- Enables graceful shutdown where processes can complete their current work and clean up properly before exiting
- Used extensively across PostgreSQL's background processes for coordinated system shutdown