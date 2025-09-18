# SignalHandlerForConfigReload

## Location
src/backend/postmaster/interrupt.c: 61 - 72

## Overview
SignalHandlerForConfigReload is a simple signal handler designed to trigger configuration reloads in PostgreSQL processes, typically in response to SIGHUP signals.

## Definition
```c
void SignalHandlerForConfigReload(SIGNAL_ARGS)
```

## Detailed Description
This function serves as a lightweight signal handler specifically designed to handle configuration reload requests. When invoked (typically by a SIGHUP signal), it sets the global ConfigReloadPending flag to true and wakes up the process by setting its latch. The actual configuration reload processing is deferred to be handled later in the main loop, either through explicit checks of the ConfigReloadPending flag or by calling HandleMainLoopInterrupts.

This design pattern allows PostgreSQL processes to handle configuration reload signals safely without performing complex operations directly within the signal handler context, which could lead to race conditions or other signal safety issues.

## Parameters / Member Variables
- Uses SIGNAL_ARGS macro which typically expands to signal handler parameters (signal number, etc.)

## Dependencies
- Functions called/Symbols referenced:
  - SIGNAL_ARGS (signal handler parameter macro)
  - SetLatch (to wake up the process)
- Called from (representative examples):
  - AutoVacWorkerMain (src/backend/postmaster/autovacuum.c:1383)
  - BackgroundWriterMain (src/backend/postmaster/bgwriter.c:102)
  - CheckpointerMain (src/backend/postmaster/checkpointer.c:196)
  - PgArchiverMain (src/backend/postmaster/pgarch.c:228)
  - SysLoggerMain (src/backend/postmaster/syslogger.c:279)
  - WalSummarizerMain (src/backend/postmaster/walsummarizer.c:247)
  - WalWriterMain (src/backend/postmaster/walwriter.c:107)
  - PostgresMain (src/backend/tcop/postgres.c:4272)
  - Various replication workers and other background processes

## Notes and Other Information
- This signal handler is declared in src/include/postmaster/interrupt.h for use across PostgreSQL
- The handler follows safe signal handling practices by doing minimal work (just setting flags and latches)
- It's commonly used with SIGHUP to allow runtime configuration changes without restarting processes
- The ConfigReloadPending flag it sets is typically checked by HandleMainLoopInterrupts or similar interrupt handling functions
- The SetLatch call ensures that processes waiting on their latch will wake up to process the pending configuration reload
- Used extensively throughout PostgreSQL's background processes, demonstrating its importance for runtime configuration management