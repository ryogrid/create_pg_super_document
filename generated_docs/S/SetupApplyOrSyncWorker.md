# SetupApplyOrSyncWorker

## Location
[src/backend/replication/logical/worker.c:4691-4744](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/worker.c#L4691-L4744)

## Overview
Common setup function for both leader apply workers and tablesync workers that handles worker attachment, signal configuration, library loading, and callback registration.

## Definition

```c
void
SetupApplyOrSyncWorker(int worker_slot)
```
## Detailed Description
This function performs the common initialization sequence shared between leader apply workers and table synchronization workers. It establishes the runtime environment necessary for logical replication processing:

1. **Worker Attachment**: Attaches to the specified logical replication worker slot
2. **Worker Type Validation**: Ensures the worker is either a tablesync or leader apply worker
3. **Signal Handling Setup**: Configures signal handlers for configuration reload (SIGHUP) and termination (SIGTERM)
4. **Statistics Initialization**: Sets initial timestamps for communication tracking  
5. **Library Loading**: Loads the libpqwalreceiver library for PostgreSQL connection handling
6. **Worker Initialization**: Calls InitializeLogRepWorker for database connection and subscription setup
7. **Exit Callback Registration**: Registers replorigin_reset to clean up replication origin state on shutdown
8. **Cache Callback Setup**: Registers for subscription relation state changes

The function ensures proper resource management and cleanup by registering exit callbacks that prevent incomplete transactions from affecting replication origin advancement.

## Parameters / Member Variables
- : The worker slot number to attach to, identifying the specific worker instance

Global variables accessed:
- : Worker information structure populated after slot attachment
- : Subscription configuration loaded during initialization

## Dependencies
- Functions called/Symbols referenced:
  - [logicalrep_worker_attach](../l/logicalrep_worker_attach.md): Attach to logical replication worker slot
  - [am_tablesync_worker](../a/am_tablesync_worker.md)/am_leader_apply_worker: Worker type identification functions
  - [pqsignal](../p/pqsignal.md): Register signal handlers for SIGHUP and SIGTERM
  - [SignalHandlerForConfigReload](SignalHandlerForConfigReload.md)/die: Signal handler functions
  - [BackgroundWorkerUnblockSignals](../B/BackgroundWorkerUnblockSignals.md): Enable signal processing
  - [GetCurrentTimestamp](../G/GetCurrentTimestamp.md): Initialize communication timestamps
  - [load_file](../l/load_file.md): Load libpqwalreceiver library
  - [InitializeLogRepWorker](../I/InitializeLogRepWorker.md): Common worker initialization
  - [before_shmem_exit](../b/before_shmem_exit.md): Register shutdown callback
  - [replorigin_reset](../r/replorigin_reset.md): Origin state cleanup callback
  - [CacheRegisterSyscacheCallback](../C/CacheRegisterSyscacheCallback.md): Register for catalog change notifications
  - [invalidate_syncing_table_states](../i/invalidate_syncing_table_states.md): Cache invalidation callback
- Called from:
  - [TablesyncWorkerMain](../T/TablesyncWorkerMain.md): Table synchronization worker main function
  - [ApplyWorkerMain](../A/ApplyWorkerMain.md): Apply worker main function

## Notes and Other Information
- This is a public function that can be called from other source files
- Used by both apply workers and tablesync workers, providing common initialization logic
- Signal handling is critical for graceful worker shutdown and configuration updates
- The libpqwalreceiver library is dynamically loaded to handle PostgreSQL connections
- Exit callback registration prevents replication origin corruption during shutdown
- Statistics initialization ensures accurate tracking of communication timestamps
- Cache callback registration enables dynamic response to subscription configuration changes
- Includes comprehensive debug logging for connection establishment
- Properly handles resource management with attention to shutdown scenarios where incomplete transactions could affect replication consistency

## Simplified Source

```c
void
SetupApplyOrSyncWorker(int worker_slot)
{
    // Attach to the specified worker slot
    logicalrep_worker_attach(worker_slot);

    // Validate worker type
    Assert(am_tablesync_worker() || am_leader_apply_worker());

    // Setup signal handling
    pqsignal(SIGHUP, SignalHandlerForConfigReload);
    pqsignal(SIGTERM, die);
    BackgroundWorkerUnblockSignals();

    // Initialize communication statistics
    MyLogicalRepWorker->last_send_time = MyLogicalRepWorker->last_recv_time =
        MyLogicalRepWorker->reply_time = GetCurrentTimestamp();

    // Load PostgreSQL connection library
    load_file("libpqwalreceiver", false);

    // Perform common worker initialization
    InitializeLogRepWorker();

    // Register exit callback to clean up origin state on shutdown
    // This prevents incomplete transactions from affecting origin advancement
    before_shmem_exit(replorigin_reset, (Datum) 0);

    // Debug log connection attempt
    elog(DEBUG1, "connecting to publisher using connection string \"%s\"",
         MySubscription->conninfo);

    // Register for subscription relation state changes
    CacheRegisterSyscacheCallback(SUBSCRIPTIONRELMAP,
                                  invalidate_syncing_table_states,
                                  (Datum) 0);
}
```