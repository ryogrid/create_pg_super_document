# BackgroundWriterMain

## Location
[src/backend/postmaster/bgwriter.c:87-342](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/postmaster/bgwriter.c#L87-L342)

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
  - [BgBufferSync](BgBufferSync.md)
  - [SignalHandlerForConfigReload](../S/SignalHandlerForConfigReload.md)
  - [SignalHandlerForShutdownRequest](../S/SignalHandlerForShutdownRequest.md)
  - [procsignal_sigusr1_handler](../p/procsignal_sigusr1_handler.md)
  - AllocSetContextCreate
  - [WritebackContextInit](../W/WritebackContextInit.md)
  - [GetCurrentTimestamp](../G/GetCurrentTimestamp.md)
  - [pgstat_report_bgwriter](../p/pgstat_report_bgwriter.md)
  - [pgstat_report_wal](../p/pgstat_report_wal.md)
  - [FirstCallSinceLastCheckpoint](../F/FirstCallSinceLastCheckpoint.md)
  - [smgrdestroyall](../s/smgrdestroyall.md)
  - XLogStandbyInfoActive
  - [LogStandbySnapshot](../L/LogStandbySnapshot.md)
  - [WaitLatch](../W/WaitLatch.md)
  - [StrategyNotifyBgWriter](../S/StrategyNotifyBgWriter.md)
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

## Simplified Source

```c
// Simplified version of BackgroundWriterMain
void BackgroundWriterMain(char *startup_data, size_t startup_data_len) {
    sigjmp_buf local_sigjmp_buf;
    MemoryContext bgwriter_context;
    bool prev_hibernate;
    WritebackContext wb_context;

    // Initialize process type and common auxiliary setup
    MyBackendType = B_BG_WRITER;
    AuxiliaryProcessMainCommon();

    // Set up signal handlers for proper process management
    pqsignal(SIGHUP, SignalHandlerForConfigReload);
    pqsignal(SIGTERM, SignalHandlerForShutdownRequest);
    pqsignal(SIGUSR1, procsignal_sigusr1_handler);
    // Other signals ignored or set to default

    // Initialize timing for snapshot logging
    last_snapshot_ts = GetCurrentTimestamp();

    // Create dedicated memory context for safe error recovery
    bgwriter_context = AllocSetContextCreate(TopMemoryContext,
                                           "Background Writer",
                                           ALLOCSET_DEFAULT_SIZES);
    MemoryContextSwitchTo(bgwriter_context);
    WritebackContextInit(&wb_context, &bgwriter_flush_after);

    // Error recovery point using sigsetjmp for signal safety
    if (sigsetjmp(local_sigjmp_buf, 1) != 0) {
        // Clean up after any error
        error_context_stack = NULL;
        HOLD_INTERRUPTS();
        EmitErrorReport();

        // Release resources (locks, buffers, files, etc.)
        LWLockReleaseAll();
        UnlockBuffers();
        ReleaseAuxProcessResources(false);
        // Additional cleanup calls...

        // Reset memory context and reinitialize
        MemoryContextSwitchTo(bgwriter_context);
        FlushErrorState();
        MemoryContextReset(bgwriter_context);
        WritebackContextInit(&wb_context, &bgwriter_flush_after);

        RESUME_INTERRUPTS();
        pg_usleep(1000000L); // Sleep 1 second after error
        pgstat_report_wait_end();
    }

    // Enable exception handling and unblock signals
    PG_exception_stack = &local_sigjmp_buf;
    sigprocmask(SIG_SETMASK, &UnBlockSig, NULL);
    prev_hibernate = false;

    // Main processing loop
    for (;;) {
        bool can_hibernate;
        int rc;

        // Clear pending wakeups and handle interrupts
        ResetLatch(MyLatch);
        HandleMainLoopInterrupts();

        // Core work: synchronize dirty buffers to disk
        can_hibernate = BgBufferSync(&wb_context);

        // Report statistics to the stats collector
        pgstat_report_bgwriter();
        pgstat_report_wal(true);

        // Clean up storage manager objects after checkpoints
        if (FirstCallSinceLastCheckpoint()) {
            smgrdestroyall();
        }

        // Log running transactions snapshot for replication consistency
        if (XLogStandbyInfoActive() && !RecoveryInProgress()) {
            TimestampTz now = GetCurrentTimestamp();
            TimestampTz timeout = TimestampTzPlusMilliseconds(last_snapshot_ts,
                                                            LOG_SNAPSHOT_INTERVAL_MS);

            if (now >= timeout && last_snapshot_lsn <= GetLastImportantRecPtr()) {
                last_snapshot_lsn = LogStandbySnapshot();
                last_snapshot_ts = now;
            }
        }

        // Sleep for configured delay or until signaled
        rc = WaitLatch(MyLatch,
                      WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
                      BgWriterDelay,
                      WAIT_EVENT_BGWRITER_MAIN);

        // Hibernation logic: sleep longer when system is idle
        if (rc == WL_TIMEOUT && can_hibernate && prev_hibernate) {
            // Request notification on next buffer allocation
            StrategyNotifyBgWriter(MyProcNumber);

            // Extended sleep during idle periods
            WaitLatch(MyLatch,
                     WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
                     BgWriterDelay * HIBERNATE_FACTOR,
                     WAIT_EVENT_BGWRITER_HIBERNATE);

            // Reset notification request
            StrategyNotifyBgWriter(-1);
        }

        prev_hibernate = can_hibernate;
    }
}
```

Key simplifications made:
- Removed detailed error handling comments for clarity
- Consolidated signal setup into key handlers only
- Abstracted low-level resource cleanup details
- Simplified hibernation logic explanation
- Focused on the main execution flow
- Reduced verbose comments while maintaining essential logic