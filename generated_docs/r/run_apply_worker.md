# run_apply_worker

## Location
[src/backend/replication/logical/worker.c:4478-4589](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/worker.c#L4478-L4589)

## Overview
Runs the leader apply worker by setting up replication origin tracking, establishing connection to the publisher, and starting the streaming process for logical replication.

## Definition


## Detailed Description
This function orchestrates the startup sequence for a PostgreSQL logical replication apply worker. It performs several critical initialization steps:

1. **Replication Origin Setup**: Creates or retrieves a replication origin to track replication progress, ensuring data consistency and enabling restart from the correct position
2. **Publisher Connection**: Establishes a connection to the publisher database using the subscription's connection information and authentication requirements
3. **Two-Phase Commit Handling**: Conditionally enables two-phase commit protocol if all table synchronizations are ready and the feature is requested
4. **Streaming Initialization**: Configures streaming options and starts the logical replication stream

The function handles password authentication requirements based on subscription settings and performs proper error handling for connection failures. It also manages transaction boundaries during the setup process.

## Parameters / Member Variables
This function takes no parameters but operates on global variables:
- : Global subscription object containing connection info, slot name, and configuration
- : Global WAL receiver connection used for replication

## Dependencies
- Functions called/Symbols referenced:
  - [ReplicationOriginNameForLogicalRep](../R/ReplicationOriginNameForLogicalRep.md): Generates standardized origin name
  - [StartTransactionCommand](../S/StartTransactionCommand.md)/CommitTransactionCommand: Transaction management
  - replorigin_by_name/replorigin_create: Replication origin management
  - [replorigin_session_setup](replorigin_session_setup.md)/replorigin_session_get_progress: Session origin tracking
  - walrcv_connect/walrcv_identify_system: WAL receiver connection management
  - [set_apply_error_context_origin](../s/set_apply_error_context_origin.md): Error context setup
  - [set_stream_options](../s/set_stream_options.md): Streaming configuration
  - [AllTablesyncsReady](../A/AllTablesyncsReady.md): Check table synchronization status
  - walrcv_startstreaming: Begin replication streaming
  - [UpdateTwoPhaseState](../U/UpdateTwoPhaseState.md): Update subscription's two-phase state
  - [start_apply](../s/start_apply.md): Main replication processing loop
- Called from:
  - [ApplyWorkerMain](../A/ApplyWorkerMain.md): Main entry point for apply worker process

## Notes and Other Information
- This is a static function, internal to the worker.c file
- The function includes comprehensive error handling for missing replication slots and connection failures  
- Two-phase commit support is conditionally enabled based on table sync readiness
- The function sets up proper error context for better error reporting during replication
- Transaction snapshots are carefully managed when updating subscription metadata
- Debug logging provides visibility into the two-phase commit state transitions