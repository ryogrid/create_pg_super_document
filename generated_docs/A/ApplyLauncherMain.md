# ApplyLauncherMain

## Location
[src/backend/replication/logical/launcher.c:1135-1266](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/launcher.c#L1135-L1266)

## Overview
The main entry point and event loop for the logical replication launcher background worker process that manages subscription apply workers.

## Definition


## Detailed Description
This function implements the main event loop for the logical replication launcher process, which is responsible for starting and managing apply worker processes for logical replication subscriptions. The launcher runs as a background worker and continuously monitors enabled subscriptions, launching apply workers as needed while respecting restart throttling limits. The function establishes signal handlers, connects to the database to access the pg_subscription catalog, and enters an infinite loop where it periodically checks subscription status and launches workers. It uses a memory context per iteration to prevent memory leaks and implements intelligent wait timing to minimize CPU usage while ensuring responsive worker management.

## Parameters / Member Variables
- : Standard background worker main function parameter (Datum) - not used in this implementation

## Dependencies
- Functions called/Symbols referenced:
  - ereport, before_shmem_exit, logicalrep_launcher_onexit
  - [pqsignal](../p/pqsignal.md), SignalHandlerForConfigReload, die, BackgroundWorkerUnblockSignals
  - [BackgroundWorkerInitializeConnection](../B/BackgroundWorkerInitializeConnection.md)
  - AllocSetContextCreate, MemoryContextSwitchTo, MemoryContextDelete
  - get_subscription_list, logicalrep_worker_find, logicalrep_worker_launch
  - ApplyLauncherGetWorkerStartTime, ApplyLauncherSetWorkerStartTime
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