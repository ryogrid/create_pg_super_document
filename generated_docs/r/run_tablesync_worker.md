# run_tablesync_worker

## Location
[src/backend/replication/logical/tablesync.c:1711-1736](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/tablesync.c#L1711-L1736)

## Overview
run_tablesync_worker orchestrates the complete table synchronization workflow, from initial data copy through streaming setup to continuous catchup with the main apply worker.

## Definition
```c
static void run_tablesync_worker()
```

## Detailed Description
This function represents the main execution flow for a table synchronization worker in PostgreSQL's logical replication system. It coordinates the entire lifecycle of table synchronization, seamlessly transitioning from the initial bulk data copy phase to ongoing streaming replication.

The function operates in three distinct phases:

1. **Initial Synchronization**: Calls start_table_sync to perform the initial data copy and setup replication slots and origin tracking
2. **Streaming Configuration**: Configures streaming options including slot name and starting LSN position for continuous replication
3. **Continuous Application**: Starts the streaming apply process to catch up with changes that occurred during the initial sync

The function bridges the gap between static data copying and dynamic change streaming, ensuring seamless data consistency throughout the transition. It sets up proper error context tracking using replication origins and configures streaming parameters based on the results of the initial synchronization.

## Parameters / Member Variables
This function takes no parameters but operates on global state:
- Uses MySubscription and MyLogicalRepWorker for subscription and worker context
- Operates on the established LogRepWorkerWalRcvConn connection

## Dependencies
- Functions called/Symbols referenced:
  - [start_table_sync](../s/start_table_sync.md)
  - [ReplicationOriginNameForLogicalRep](../R/ReplicationOriginNameForLogicalRep.md)
  - [set_apply_error_context_origin](../s/set_apply_error_context_origin.md)
  - [set_stream_options](../s/set_stream_options.md)
  - walrcv_startstreaming
  - [start_apply](../s/start_apply.md)
- Called from (representative examples):
  - [TablesyncWorkerMain](../T/TablesyncWorkerMain.md)

## Notes and Other Information
- Represents the top-level workflow for table synchronization workers
- Handles the critical transition from batch copy to streaming replication
- Sets up proper error context and origin tracking for debugging and monitoring
- The function assumes all necessary connections and worker context have been established
- Integrates with PostgreSQL's WAL receiver infrastructure for streaming
- The streaming phase continues until the worker catches up with the main apply worker
- No explicit error handling as errors are managed by the calling context and start_table_sync wrapper
- The function effectively transforms a table sync worker into a streaming apply worker after initial sync completion