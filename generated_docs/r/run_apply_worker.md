# run_apply_worker

## Location
[src/backend/replication/logical/worker.c:4478-4589](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/worker.c#L4478-L4589)

## Overview
Runs the leader apply worker by setting up replication origin tracking, establishing connection to the publisher, and starting the streaming process for logical replication.

## Definition

```c
static void
run_apply_worker()
```
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
  - [replorigin_by_name](replorigin_by_name.md)/replorigin_create: Replication origin management
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

## Simplified Source

```c
static void
run_apply_worker()
{
    char originname[NAMEDATALEN];
    XLogRecPtr origin_startpos = InvalidXLogRecPtr;
    char *slotname = NULL;
    WalRcvStreamOptions options;
    RepOriginId originid;
    bool must_use_password;

    // Validate subscription has a replication slot
    slotname = MySubscription->slotname;
    if (!slotname)
        ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                       errmsg("subscription has no replication slot set")));

    // Setup replication origin tracking
    ReplicationOriginNameForLogicalRep(MySubscription->oid, InvalidOid,
                                     originname, sizeof(originname));
    StartTransactionCommand();
    originid = replorigin_by_name(originname, true);
    if (!OidIsValid(originid))
        originid = replorigin_create(originname);
    replorigin_session_setup(originid, 0);
    replorigin_session_origin = originid;
    origin_startpos = replorigin_session_get_progress(false);
    CommitTransactionCommand();

    // Connect to publisher
    must_use_password = MySubscription->passwordrequired &&
                       !MySubscription->ownersuperuser;
    LogRepWorkerWalRcvConn = walrcv_connect(MySubscription->conninfo, true,
                                          true, must_use_password,
                                          MySubscription->name, &err);
    if (LogRepWorkerWalRcvConn == NULL)
        ereport(ERROR, (errcode(ERRCODE_CONNECTION_FAILURE),
                       errmsg("could not connect to the publisher: %s", err)));

    // Initialize connection and setup streaming options
    walrcv_identify_system(LogRepWorkerWalRcvConn, &startpointTLI);
    set_apply_error_context_origin(originname);
    set_stream_options(&options, slotname, &origin_startpos);

    // Handle two-phase commit if ready
    if (MySubscription->twophasestate == LOGICALREP_TWOPHASE_STATE_PENDING &&
        AllTablesyncsReady()) {
        options.proto.logical.twophase = true;
        walrcv_startstreaming(LogRepWorkerWalRcvConn, &options);

        // Update subscription state to enabled
        StartTransactionCommand();
        PushActiveSnapshot(GetTransactionSnapshot());
        UpdateTwoPhaseState(MySubscription->oid, LOGICALREP_TWOPHASE_STATE_ENABLED);
        MySubscription->twophasestate = LOGICALREP_TWOPHASE_STATE_ENABLED;
        PopActiveSnapshot();
        CommitTransactionCommand();
    } else {
        walrcv_startstreaming(LogRepWorkerWalRcvConn, &options);
    }

    // Start the main replication loop
    start_apply(origin_startpos);
}
```