# ParallelWorkerMain

## Location
[src/backend/access/transam/parallel.c:1288-1572](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/parallel.c#L1288-L1572)

## Overview
ParallelWorkerMain is the main entrypoint function for parallel worker processes, responsible for initializing and setting up the entire execution environment for a parallel worker before executing the worker-specific code.

## Definition
```c
void ParallelWorkerMain(Datum main_arg)
```

## Detailed Description
ParallelWorkerMain serves as the primary initialization function for PostgreSQL parallel worker processes. It performs comprehensive setup to establish an execution environment that mirrors the state of the parallel leader process. The function handles dynamic shared memory attachment, transaction state restoration, security context setup, and numerous other initialization tasks required for parallel query execution.

The function operates in several key phases:
1. **Signal handling and worker identification** - Sets up signal handlers and determines the worker number
2. **Dynamic shared memory attachment** - Attaches to the DSM segment created by the leader
3. **Error reporting setup** - Establishes message queues for error communication with the leader
4. **Lock group membership** - Joins the parallel lock group to prevent deadlocks
5. **State restoration** - Restores transaction state, GUC values, snapshots, and various backend states
6. **Worker execution** - Calls the application-specific parallel worker function
7. **Cleanup** - Performs shutdown procedures and reports completion

The function ensures that the parallel worker operates in an environment that is functionally equivalent to the leader process, enabling transparent execution of parallel operations.

## Parameters / Member Variables
- `main_arg`: A Datum containing the DSM segment handle (as UInt32) that the worker should attach to for accessing shared state

## Dependencies
- Functions called/Symbols referenced:
  - [dsm_attach](../d/dsm_attach.md), dsm_segment_address
  - [shm_toc_attach](../s/shm_toc_attach.md), shm_toc_lookup
  - [LookupParallelWorkerFunction](../L/LookupParallelWorkerFunction.md)
  - [SetParallelStartTimestamps](../S/SetParallelStartTimestamps.md)
  - [StartParallelWorkerTransaction](../S/StartParallelWorkerTransaction.md), EndParallelWorkerTransaction
  - [AttachSession](../A/AttachSession.md), DetachSession  
  - [RestorePendingSyncs](../R/RestorePendingSyncs.md), RestoreUncommittedEnums
  - [BecomeLockGroupMember](../B/BecomeLockGroupMember.md)
  - [BackgroundWorkerInitializeConnectionByOid](../B/BackgroundWorkerInitializeConnectionByOid.md)
  - [EnterParallelMode](../E/EnterParallelMode.md), ExitParallelMode
  - [StartTransactionCommand](../S/StartTransactionCommand.md), CommitTransactionCommand
- Called from (representative examples):
  - [BackgroundWorkerHandle](../B/BackgroundWorkerHandle.md) (via bgworker registration)
  - IsParallelWorker (helper function)

## Notes and Other Information
- Sets the global flag `InitializingParallelWorker = true` during initialization phase
- Uses the `ParallelWorkerShutdown` function as a before_shmem_exit callback for cleanup
- Redirects error messages to shared message queues for leader process consumption
- Performs extensive state restoration including GUCs, snapshots, security contexts, and various subsystem states
- The worker number is embedded in `MyBgworkerEntry->bgw_extra` and copied to `ParallelWorkerNumber`
- Creates a dedicated memory context "Parallel worker" for cleanliness during execution
- Handles both REPEATABLE READ/SERIALIZABLE transaction snapshots and lower isolation levels appropriately
- Must successfully join the lock group or exits silently to prevent deadlocks

## Simplified Source

```c
void ParallelWorkerMain(Datum main_arg)
{
    // Phase 1: Initialize worker environment
    InitializingParallelWorker = true;
    pqsignal(SIGTERM, die);
    BackgroundWorkerUnblockSignals();

    // Set worker number from background worker entry
    memcpy(&ParallelWorkerNumber, MyBgworkerEntry->bgw_extra, sizeof(int));

    // Create memory context for worker
    CurrentMemoryContext = AllocSetContextCreate(TopMemoryContext, "Parallel worker",
                                                 ALLOCSET_DEFAULT_SIZES);

    // Phase 2: Attach to dynamic shared memory
    dsm_segment *seg = dsm_attach(DatumGetUInt32(main_arg));
    if (seg == NULL)
        ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                       errmsg("could not map dynamic shared memory segment")));

    shm_toc *toc = shm_toc_attach(PARALLEL_MAGIC, dsm_segment_address(seg));
    if (toc == NULL)
        ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                       errmsg("invalid magic number in dynamic shared memory segment")));

    // Phase 3: Set up error reporting and shutdown handling
    FixedParallelState *fps = shm_toc_lookup(toc, PARALLEL_KEY_FIXED, false);
    MyFixedParallelState = fps;
    ParallelLeaderPid = fps->parallel_leader_pid;
    ParallelLeaderProcNumber = fps->parallel_leader_proc_number;
    before_shmem_exit(ParallelWorkerShutdown, PointerGetDatum(seg));

    // Set up error message queue to leader
    char *error_queue_space = shm_toc_lookup(toc, PARALLEL_KEY_ERROR_QUEUE, false);
    shm_mq *mq = (shm_mq *)(error_queue_space + ParallelWorkerNumber * PARALLEL_ERROR_QUEUE_SIZE);
    shm_mq_set_sender(mq, MyProc);
    shm_mq_handle *mqh = shm_mq_attach(mq, seg, NULL);
    pq_redirect_to_shm_mq(seg, mqh);
    pq_set_parallel_leader(fps->parallel_leader_pid, fps->parallel_leader_proc_number);

    // Phase 4: Join lock group to prevent deadlocks
    if (!BecomeLockGroupMember(fps->parallel_leader_pgproc, fps->parallel_leader_pid))
        return;

    // Phase 5: Restore various states from leader
    SetParallelStartTimestamps(fps->xact_ts, fps->stmt_ts);

    // Look up and get worker function entry point
    char *entrypointstate = shm_toc_lookup(toc, PARALLEL_KEY_ENTRYPOINT, false);
    char *library_name = entrypointstate;
    char *function_name = entrypointstate + strlen(library_name) + 1;
    parallel_worker_main_type entrypt = LookupParallelWorkerFunction(library_name, function_name);

    // Restore security context
    SetAuthenticatedUserId(fps->authenticated_user_id);
    SetSessionAuthorization(fps->session_user_id, fps->session_user_is_superuser);
    SetCurrentRoleId(fps->outer_user_id, fps->role_is_superuser);

    // Initialize database connection
    BackgroundWorkerInitializeConnectionByOid(fps->database_id, fps->authenticated_user_id,
                                              BGWORKER_BYPASS_ALLOWCONN | BGWORKER_BYPASS_ROLELOGINCHECK);
    SetClientEncoding(GetDatabaseEncoding());

    // Restore libraries and GUCs
    char *libraryspace = shm_toc_lookup(toc, PARALLEL_KEY_LIBRARY, false);
    StartTransactionCommand();
    RestoreLibraryState(libraryspace);

    char *gucspace = shm_toc_lookup(toc, PARALLEL_KEY_GUC, false);
    RestoreGUCState(gucspace);
    CommitTransactionCommand();

    // Phase 6: Restore transaction and snapshot state
    char *tstatespace = shm_toc_lookup(toc, PARALLEL_KEY_TRANSACTION_STATE, false);
    StartParallelWorkerTransaction(tstatespace);

    char *combocidspace = shm_toc_lookup(toc, PARALLEL_KEY_COMBO_CID, false);
    RestoreComboCIDState(combocidspace);

    // Attach to session and restore snapshots
    char *session_dsm_handle_space = shm_toc_lookup(toc, PARALLEL_KEY_SESSION_DSM, false);
    AttachSession(*(dsm_handle *)session_dsm_handle_space);

    char *asnapspace = shm_toc_lookup(toc, PARALLEL_KEY_ACTIVE_SNAPSHOT, false);
    char *tsnapspace = shm_toc_lookup(toc, PARALLEL_KEY_TRANSACTION_SNAPSHOT, true);
    Snapshot asnapshot = RestoreSnapshot(asnapspace);
    Snapshot tsnapshot = tsnapspace ? RestoreSnapshot(tsnapspace) : asnapshot;
    RestoreTransactionSnapshot(tsnapshot, fps->parallel_leader_pgproc);
    PushActiveSnapshot(asnapshot);

    // Phase 7: Final state restoration
    InvalidateSystemCaches();
    SetUserIdAndSecContext(fps->current_user_id, fps->sec_context);
    SetTempNamespaceState(fps->temp_namespace_id, fps->temp_toast_namespace_id);

    // Restore various subsystem states
    char *pendingsyncsspace = shm_toc_lookup(toc, PARALLEL_KEY_PENDING_SYNCS, false);
    RestorePendingSyncs(pendingsyncsspace);

    // Restore other states (reindex, relmapper, enums, client info)
    // ... (multiple RestoreXXX calls)

    if (MyClientConnectionInfo.authn_id)
        InitializeSystemUser(MyClientConnectionInfo.authn_id,
                           hba_authname(MyClientConnectionInfo.auth_method));

    AttachSerializableXact(fps->serializable_xact_handle);

    // Phase 8: Execute worker function
    InitializingParallelWorker = false;
    EnterParallelMode();

    entrypt(seg, toc);  // Call the actual worker function

    // Phase 9: Cleanup
    ExitParallelMode();
    PopActiveSnapshot();
    EndParallelWorkerTransaction();
    DetachSession();
    pq_putmessage(PqMsg_Terminate, NULL, 0);
}
```