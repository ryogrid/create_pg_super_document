# SignalHandlerForConfigReload

## Location
[src/backend/postmaster/interrupt.c:61-72](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/postmaster/interrupt.c#L61-L72)

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


## Dependencies
- Functions called/Symbols referenced:
  - SIGNAL_ARGS (signal handler parameter macro)
  - [SetLatch](SetLatch.md) (to wake up the process)
- Called from (representative examples):
  - [AutoVacWorkerMain](../A/AutoVacWorkerMain.md) (src/backend/postmaster/autovacuum.c:1383)
  - [BackgroundWriterMain](../B/BackgroundWriterMain.md) (src/backend/postmaster/bgwriter.c:102)
  - [CheckpointerMain](../C/CheckpointerMain.md) (src/backend/postmaster/checkpointer.c:196)
  - [PgArchiverMain](../P/PgArchiverMain.md) (src/backend/postmaster/pgarch.c:228)
  - [SysLoggerMain](SysLoggerMain.md) (src/backend/postmaster/syslogger.c:279)
  - [WalSummarizerMain](../W/WalSummarizerMain.md) (src/backend/postmaster/walsummarizer.c:247)
  - [WalWriterMain](../W/WalWriterMain.md) (src/backend/postmaster/walwriter.c:107)
  - [PostgresMain](../P/PostgresMain.md) (src/backend/tcop/postgres.c:4272)
  - Various replication workers and other background processes

## Notes and Other Information
- This signal handler is declared in src/include/postmaster/interrupt.h for use across PostgreSQL
- The handler follows safe signal handling practices by doing minimal work (just setting flags and latches)
- It's commonly used with SIGHUP to allow runtime configuration changes without restarting processes
- The ConfigReloadPending flag it sets is typically checked by HandleMainLoopInterrupts or similar interrupt handling functions
- The SetLatch call ensures that processes waiting on their latch will wake up to process the pending configuration reload
- Used extensively throughout PostgreSQL's background processes, demonstrating its importance for runtime configuration management

## Simplified Source

```c
// Simplified version of SignalHandlerForConfigReload
void SignalHandlerForConfigReload(SIGNAL_ARGS) {
    // Step 1: Mark that a configuration reload is needed
    ConfigReloadPending = true;

    // Step 2: Wake up the main process to handle the reload
    SetLatch(MyLatch);
}
```

Key simplifications made:
- Added inline comments explaining the two core actions
- The function is already minimal and safe for signal handling
- Preserved the essential two-step pattern: flag setting + process wakeup
- No complex logic to simplify - this is an exemplar of good signal handler design