# LogicalRepSyncTableStart

## Location
[src/backend/replication/logical/tablesync.c:1309-1597](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/tablesync.c#L1309-L1597)

## Overview
LogicalRepSyncTableStart orchestrates the initial table synchronization phase for logical replication, establishing replication slots, performing data copying, and setting up origin tracking to ensure consistent table state replication.

## Definition
```c
static char *LogicalRepSyncTableStart(XLogRecPtr *origin_startpos)
```

## Detailed Description
This is the core function responsible for initiating and managing table synchronization in PostgreSQL's logical replication system. It handles the complex process of synchronizing a single table from the publisher to the subscriber, ensuring data consistency and proper state management throughout the operation.

The function performs several critical phases:

1. **State Assessment**: Checks the current synchronization state and determines if work needs to be done
2. **Connection Management**: Establishes a dedicated WAL receiver connection to the publisher
3. **Slot Creation**: Creates a temporary replication slot for consistent data capture during copy
4. **Origin Tracking Setup**: Establishes replication origin tracking for crash recovery and progress monitoring
5. **Permission Validation**: Ensures proper access controls and RLS compliance
6. **Data Copy**: Performs the initial COPY operation to transfer existing table data
7. **State Transitions**: Updates synchronization state through various phases (DATASYNC, FINISHEDCOPY, SYNCWAIT)

The function implements sophisticated error handling and recovery mechanisms, including cleanup of partially created replication slots and proper transaction boundary management.

## Parameters / Member Variables
- `origin_startpos`: Output parameter that receives the LSN position from which to start logical replication after the initial copy phase

## Dependencies
- Functions called/Symbols referenced:
  - [StartTransactionCommand](../S/StartTransactionCommand.md)/CommitTransactionCommand
  - [GetSubscriptionRelState](../G/GetSubscriptionRelState.md)
  - [ReplicationSlotNameForTablesync](../R/ReplicationSlotNameForTablesync.md)
  - walrcv_connect
  - [ReplicationSlotDropAtPubNode](../R/ReplicationSlotDropAtPubNode.md)
  - [UpdateSubscriptionRelState](../U/UpdateSubscriptionRelState.md)
  - walrcv_create_slot
  - [replorigin_create](../r/replorigin_create.md)/replorigin_session_setup
  - [copy_table](../c/copy_table.md)
  - [wait_for_worker_state_change](../w/wait_for_worker_state_change.md)
- Called from (representative examples):
  - [start_table_sync](../s/start_table_sync.md)

## Notes and Other Information
- The function uses REPEATABLE READ isolation level on the publisher to ensure consistency between slot creation and data copy
- Implements comprehensive permission checking including ACL verification and Row Level Security (RLS) compliance
- Supports both superuser and non-superuser subscription owners with appropriate privilege switching
- Creates permanent replication slots that persist beyond the initial copy phase for ongoing synchronization
- The returned slot name is palloc'd and must be managed by the caller
- Handles recovery scenarios where previous synchronization attempts were interrupted
- Integrates with PostgreSQL's transaction system and statistics reporting mechanisms

## Simplified Source

```c
static char *LogicalRepSyncTableStart(XLogRecPtr *origin_startpos)
{
    char *slotname;
    char relstate;
    XLogRecPtr relstate_lsn;
    Relation rel;
    AclResult aclresult;
    WalRcvExecResult *res;
    char originname[NAMEDATALEN];
    RepOriginId originid;
    UserContext ucxt;
    bool must_use_password;
    bool run_as_owner;

    // Step 1: Check current synchronization state
    StartTransactionCommand();
    relstate = GetSubscriptionRelState(MyLogicalRepWorker->subid,
                                      MyLogicalRepWorker->relid,
                                      &relstate_lsn);
    CommitTransactionCommand();

    // Update worker state in shared memory
    SpinLockAcquire(&MyLogicalRepWorker->relmutex);
    MyLogicalRepWorker->relstate = relstate;
    MyLogicalRepWorker->relstate_lsn = relstate_lsn;
    SpinLockRelease(&MyLogicalRepWorker->relmutex);

    // Step 2: Exit if synchronization already complete
    switch (relstate) {
        case SUBREL_STATE_SYNCDONE:
        case SUBREL_STATE_READY:
        case SUBREL_STATE_UNKNOWN:
            finish_sync_worker();  // doesn't return
    }

    // Step 3: Set up replication slot name
    slotname = palloc(NAMEDATALEN);
    ReplicationSlotNameForTablesync(MySubscription->oid,
                                   MyLogicalRepWorker->relid,
                                   slotname, NAMEDATALEN);

    // Step 4: Handle password requirements and user context
    must_use_password = MySubscription->passwordrequired &&
                       !MySubscription->ownersuperuser;
    run_as_owner = !MySubscription->ownersuperuser;

    // Step 5: Establish connection to publisher
    LogRepWorkerWalRcvConn = walrcv_connect(MySubscription->conninfo,
                                           true, must_use_password,
                                           MySubscription->name, &err);
    if (LogRepWorkerWalRcvConn == NULL)
        ereport(ERROR, (errcode(ERRCODE_CONNECTION_FAILURE),
                errmsg("could not connect to the publisher: %s", err)));

    // Step 6: Set up user context if needed
    if (run_as_owner) {
        SwitchToUntrustedUser(MySubscription->owner, &ucxt);
    }

    // Step 7: Open and validate the local relation
    rel = table_open(MyLogicalRepWorker->relid, RowExclusiveLock);

    // Check permissions
    aclresult = pg_class_aclcheck(MyLogicalRepWorker->relid,
                                 GetUserId(), ACL_INSERT);
    if (aclresult != ACLCHECK_OK)
        aclcheck_error(aclresult, OBJECT_TABLE, get_rel_name(MyLogicalRepWorker->relid));

    // Step 8: Create replication slot if needed
    if (relstate == SUBREL_STATE_INIT) {
        // Create logical replication slot on publisher
        res = walrcv_create_slot(LogRepWorkerWalRcvConn, slotname, false,
                                false, false, CRS_NOEXPORT_SNAPSHOT, origin_startpos);

        // Update state to DATASYNC
        StartTransactionCommand();
        UpdateSubscriptionRelState(MyLogicalRepWorker->subid,
                                  MyLogicalRepWorker->relid,
                                  SUBREL_STATE_DATASYNC, *origin_startpos);
        CommitTransactionCommand();
    }

    // Step 9: Set up replication origin tracking
    ReplicationOriginNameForLogicalRep(MySubscription->oid,
                                      MyLogicalRepWorker->relid,
                                      originname, sizeof(originname));

    StartTransactionCommand();
    originid = replorigin_create(originname);
    replorigin_session_setup(originid);
    replorigin_session_origin = originid;
    CommitTransactionCommand();

    // Step 10: Perform initial data copy
    copy_table(rel);

    // Step 11: Update state to FINISHEDCOPY
    StartTransactionCommand();
    UpdateSubscriptionRelState(MyLogicalRepWorker->subid,
                              MyLogicalRepWorker->relid,
                              SUBREL_STATE_FINISHEDCOPY, *origin_startpos);
    CommitTransactionCommand();

    // Step 12: Wait for apply worker to catch up
    wait_for_worker_state_change(SUBREL_STATE_SYNCWAIT);

    // Step 13: Cleanup
    table_close(rel, NoLock);
    if (run_as_owner)
        RestoreUserContext(&ucxt);

    return slotname;
}
```