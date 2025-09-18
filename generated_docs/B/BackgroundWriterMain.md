# BackgroundWriterMain

## Location
src/backend/postmaster/bgwriter.c: 87 - 342

## Overview
BackgroundWriterMain is the main entry point for PostgreSQL's background writer process, responsible for continuously writing dirty buffers to disk to maintain system performance and reduce checkpoint overhead.

## Definition
```c
void BackgroundWriterMain(char *startup_data, size_t startup_data_len)
```

## Detailed Description
BackgroundWriterMain implements the core functionality of PostgreSQL's background writer daemon process. The background writer is a critical component that continuously scans the shared buffer pool and writes dirty pages to disk, reducing the I/O burden during checkpoints and improving overall system performance.

The function establishes a robust execution environment with comprehensive error handling using sigsetjmp/longjmp. It runs in an infinite loop, periodically calling BgBufferSync() to perform the actual buffer writing work. The process includes sophisticated hibernation logic to save power when the system is idle, and integrates with PostgreSQL's statistics reporting and WAL logging systems.

Key responsibilities include:
- Setting up signal handlers for process management
- Creating a dedicated memory context for safe error recovery
- Implementing the main processing loop with hibernation support
- Coordinating with the checkpoint process and standby logging
- Managing buffer synchronization and writeback operations

## Parameters / Member Variables
- `startup_data`: Startup data passed from the postmaster process (currently unused, expected to be NULL)
- `startup_data_len`: Length of startup data (expected to be 0)

## Dependencies
- Functions called/Symbols referenced:
  - [AuxiliaryProcessMainCommon](../A/AuxiliaryProcessMainCommon.md)
  - BgBufferSync
  - [SignalHandlerForConfigReload](../S/SignalHandlerForConfigReload.md)
  - [SignalHandlerForShutdownRequest](../S/SignalHandlerForShutdownRequest.md)
  - [procsignal_sigusr1_handler](../p/procsignal_sigusr1_handler.md)
  - AllocSetContextCreate
  - [WritebackContextInit](../W/WritebackContextInit.md)
  - [GetCurrentTimestamp](../G/GetCurrentTimestamp.md)
  - [pgstat_report_bgwriter](../p/pgstat_report_bgwriter.md)
  - pgstat_report_wal
  - [FirstCallSinceLastCheckpoint](../F/FirstCallSinceLastCheckpoint.md)
  - [smgrdestroyall](../s/smgrdestroyall.md)
  - XLogStandbyInfoActive
  - LogStandbySnapshot
  - [WaitLatch](../W/WaitLatch.md)
  - StrategyNotifyBgWriter
  - Various memory management and error handling functions

- Called from (representative examples):
  - child_process_kind (launch_backend.c:203)
  - Referenced in bgwriter.h header file

## Notes and Other Information
- The background writer uses a hibernation mechanism to reduce CPU usage during idle periods, sleeping for extended periods (BgWriterDelay * HIBERNATE_FACTOR) when no buffer activity is detected
- Error recovery is implemented using sigsetjmp/longjmp rather than PG_TRY/PG_CATCH to ensure proper signal handling at the bottom of the exception stack
- The process periodically logs running transaction snapshots (every LOG_SNAPSHOT_INTERVAL_MS) to assist with replication consistency
- Signal handling is carefully configured to respond to configuration reloads (SIGHUP) and shutdown requests (SIGTERM) while ignoring other signals
- Memory management uses a dedicated context that can be safely reset during error recovery
- The function integrates closely with the buffer management strategy and checkpoint processes