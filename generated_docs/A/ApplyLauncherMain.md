# ApplyLauncherMain

## Location
[src/backend/replication/logical/launcher.c:1135-1266](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/launcher.c#L1135-L1266)

## Overview
The main entry point and event loop for the logical replication launcher background worker process that manages subscription apply workers.

## Definition

```c
void
ApplyLauncherMain(Datum main_arg)
```
## Detailed Description
This function implements the main event loop for the logical replication launcher process, which is responsible for starting and managing apply worker processes for logical replication subscriptions. The launcher runs as a background worker and continuously monitors enabled subscriptions, launching apply workers as needed while respecting restart throttling limits. The function establishes signal handlers, connects to the database to access the pg_subscription catalog, and enters an infinite loop where it periodically checks subscription status and launches workers. It uses a memory context per iteration to prevent memory leaks and implements intelligent wait timing to minimize CPU usage while ensuring responsive worker management.

## Parameters / Member Variables
- `main_arg`: Standard background worker main function parameter (Datum) - not used in this implementation
## Dependencies
- Functions called/Symbols referenced:
  - ereport, before_shmem_exit, logicalrep_launcher_onexit
  - [pqsignal](../p/pqsignal.md), SignalHandlerForConfigReload, die, BackgroundWorkerUnblockSignals
  - [BackgroundWorkerInitializeConnection](../B/BackgroundWorkerInitializeConnection.md)
  - AllocSetContextCreate, MemoryContextSwitchTo, MemoryContextDelete
  - [get_subscription_list](../g/get_subscription_list.md), logicalrep_worker_find, logicalrep_worker_launch
  - [ApplyLauncherGetWorkerStartTime](ApplyLauncherGetWorkerStartTime.md), ApplyLauncherSetWorkerStartTime
  - [GetCurrentTimestamp](../G/GetCurrentTimestamp.md), TimestampDifferenceMilliseconds
  - [WaitLatch](../W/WaitLatch.md), ResetLatch, ProcessConfigFile
- Called from (representative examples):
  - [BackgroundWorkerHandle](../B/BackgroundWorkerHandle.md) (background worker infrastructure)

## Notes and Other Information
- This function never returns under normal operation - it runs until the process is terminated
- Implements restart throttling using wal_retrieve_retry_interval to prevent rapid restart loops
- Uses WaitLatch for efficient sleeping while remaining responsive to signals
- Handles SIGHUP for configuration reloads and SIGTERM for graceful shutdown
- Creates temporary memory contexts per iteration to prevent memory leaks during long-running operation
- Only launches workers for enabled subscriptions and skips subscriptions that already have running workers
- Adjusts wait times dynamically based on when workers can next be started

## Simplified Source

```c
void ApplyLauncherMain(Datum main_arg) {
    ereport(DEBUG1, (errmsg_internal("logical replication launcher started")));

    // Setup cleanup and process identification
    before_shmem_exit(logicalrep_launcher_onexit, (Datum) 0);
    LogicalRepCtx->launcher_pid = MyProcPid;

    // Setup signal handlers
    pqsignal(SIGHUP, SignalHandlerForConfigReload);
    pqsignal(SIGTERM, die);
    BackgroundWorkerUnblockSignals();

    // Connect to database (only need pg_subscription access)
    BackgroundWorkerInitializeConnection(NULL, NULL, 0);

    // Main event loop - runs indefinitely
    for (;;) {
        List *sublist;
        ListCell *lc;
        MemoryContext subctx;
        long wait_time = DEFAULT_NAPTIME_PER_CYCLE;

        CHECK_FOR_INTERRUPTS();

        // Create temporary context to prevent memory leaks
        subctx = AllocSetContextCreate(TopMemoryContext,
                                     "Logical Replication Launcher sublist",
                                     ALLOCSET_DEFAULT_SIZES);
        MemoryContextSwitchTo(subctx);

        // Check each enabled subscription and start missing workers
        sublist = get_subscription_list();
        foreach(lc, sublist) {
            Subscription *sub = (Subscription *) lfirst(lc);
            LogicalRepWorker *w;
            TimestampTz last_start, now;

            if (!sub->enabled) continue;

            // Check if worker already running
            LWLockAcquire(LogicalRepWorkerLock, LW_SHARED);
            w = logicalrep_worker_find(sub->oid, InvalidOid, false);
            LWLockRelease(LogicalRepWorkerLock);

            if (w != NULL) continue;  // Worker already running

            // Apply restart throttling - respect retry interval
            last_start = ApplyLauncherGetWorkerStartTime(sub->oid);
            now = GetCurrentTimestamp();

            if (last_start == 0 ||
                TimestampDifferenceMilliseconds(last_start, now) >= wal_retrieve_retry_interval) {
                // Launch new worker
                ApplyLauncherSetWorkerStartTime(sub->oid, now);
                if (!logicalrep_worker_launch(WORKERTYPE_APPLY, sub->dbid, sub->oid,
                                            sub->name, sub->owner, InvalidOid, DSM_HANDLE_INVALID)) {
                    // Failed to launch - wait before retry
                    wait_time = Min(wait_time, wal_retrieve_retry_interval);
                }
            } else {
                // Still in throttling period - calculate remaining wait
                long elapsed = TimestampDifferenceMilliseconds(last_start, now);
                wait_time = Min(wait_time, wal_retrieve_retry_interval - elapsed);
            }
        }

        // Cleanup temporary memory and wait for next cycle
        MemoryContextSwitchTo(TopMemoryContext);
        MemoryContextDelete(subctx);

        // Sleep until next check or signal
        int rc = WaitLatch(MyLatch, WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
                          wait_time, WAIT_EVENT_LOGICAL_LAUNCHER_MAIN);

        if (rc & WL_LATCH_SET) {
            ResetLatch(MyLatch);
            CHECK_FOR_INTERRUPTS();
        }

        // Handle configuration reload requests
        if (ConfigReloadPending) {
            ConfigReloadPending = false;
            ProcessConfigFile(PGC_SIGHUP);
        }
    }
}
```