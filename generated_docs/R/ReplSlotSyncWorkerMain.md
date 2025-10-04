# ReplSlotSyncWorkerMain

## Location
[src/backend/replication/logical/slotsync.c:1331-1509](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/slotsync.c#L1331-L1509)

## Overview
The main entry point and loop for PostgreSQL's slot synchronization worker process, responsible for establishing a connection to the primary server and continuously synchronizing logical replication slots.

## Definition

```c
struct in shared memory.  We must do this
	 * before we access any shared memory.
	 */
	InitProcess();
```
## Detailed Description
This function serves as the main entry point for the slot synchronization worker process in PostgreSQL's logical replication system. It initializes the worker process, establishes a connection to the primary server, and runs an infinite loop to continuously synchronize logical failover slots. The function handles process setup including signal handling, database connection, validation of remote server configuration, and proper cleanup on exit. It operates as a background worker that ensures standby servers maintain up-to-date copies of logical replication slots from the primary server.

## Parameters / Member Variables
- : Startup data passed to the worker (currently unused, expected to be NULL)
- : Length of startup data (expected to be 0)

## Dependencies
- Functions called/Symbols referenced:
  - [InitProcess](../I/InitProcess.md) (process initialization)
  - [BaseInit](../B/BaseInit.md) (early backend initialization)
  - [InitPostgres](../I/InitPostgres.md) (database connection initialization)
  - walrcv_connect (primary server connection)
  - synchronize_slots (core slot synchronization logic)
  - [ProcessSlotSyncInterrupts](../P/ProcessSlotSyncInterrupts.md) (interrupt handling)
  - [wait_for_slot_activity](../w/wait_for_slot_activity.md) (timing control)
  - validate_remote_info (primary server validation)
  - [check_and_set_sync_info](../c/check_and_set_sync_info.md) (sync context setup)
  - SetProcessingMode (processing state management)
  - Various signal handlers and cleanup functions

- Called from (representative examples):
  - child_process_kind (src/backend/postmaster/launch_backend.c:198)
  - Referenced in SLOTSYNC_H header (src/include/replication/slotsync.h:29)

## Notes and Other Information
- This is the main function for B_SLOTSYNC_WORKER backend type
- Implements comprehensive error handling with sigsetjmp/siglongjmp
- Establishes secure database connection as superuser for slot operations
- Runs indefinitely until terminated by SIGINT or error conditions
- Sets up proper signal handling for graceful shutdown and configuration reload
- Registers cleanup callbacks for proper resource management on exit
- Validates that the server is not a cascading standby before proceeding
- Uses walreceiver infrastructure for primary server communication
- Critical component of PostgreSQL's logical replication failover capability

## Simplified Source

```c
void ReplSlotSyncWorkerMain(char *startup_data, size_t startup_data_len)
{
    WalReceiverConn *wrconn = NULL;
    char *dbname;
    char *err;
    sigjmp_buf local_sigjmp_buf;
    StringInfoData app_name;

    MyBackendType = B_SLOTSYNC_WORKER;

    // Initialize process and backend
    init_ps_display(NULL);
    SetProcessingMode(InitProcessing);
    InitProcess();
    BaseInit();

    // Set up exception handling
    if (sigsetjmp(local_sigjmp_buf, 1) != 0)
    {
        error_context_stack = NULL;
        HOLD_INTERRUPTS();
        EmitErrorReport();
        proc_exit(0);
    }
    PG_exception_stack = &local_sigjmp_buf;

    // Set up signal handlers
    pqsignal(SIGHUP, SignalHandlerForConfigReload);
    pqsignal(SIGINT, SignalHandlerForShutdownRequest);
    pqsignal(SIGTERM, die);
    // ... other signal handlers

    check_and_set_sync_info(MyProcPid);
    ereport(LOG, errmsg("slot sync worker started"));

    // Register cleanup callback
    before_shmem_exit(slotsync_worker_onexit, (Datum) 0);

    // Initialize timeouts and load required libraries
    InitializeTimeouts();
    load_file("libpqwalreceiver", false);
    sigprocmask(SIG_SETMASK, &UnBlockSig, NULL);

    // Set secure search path
    SetConfigOption("search_path", "", PGC_SUSET, PGC_S_OVERRIDE);

    // Connect to database
    dbname = CheckAndGetDbnameFromConninfo();
    InitPostgres(dbname, InvalidOid, NULL, InvalidOid, 0, NULL);
    SetProcessingMode(NormalProcessing);

    // Build application name
    initStringInfo(&app_name);
    if (cluster_name[0])
        appendStringInfo(&app_name, "%s_%s", cluster_name, "slotsync worker");
    else
        appendStringInfoString(&app_name, "slotsync worker");

    // Connect to primary server
    wrconn = walrcv_connect(PrimaryConnInfo, false, false, false,
                           app_name.data, &err);
    pfree(app_name.data);

    if (!wrconn)
        ereport(ERROR,
                errcode(ERRCODE_CONNECTION_FAILURE),
                errmsg("could not connect to the primary server: %s", err));

    // Register disconnect callback
    before_shmem_exit(slotsync_worker_disconnect, PointerGetDatum(wrconn));

    // Validate remote server configuration
    validate_remote_info(wrconn);

    // Main synchronization loop
    for (;;)
    {
        bool some_slot_updated = false;

        ProcessSlotSyncInterrupts(wrconn);
        some_slot_updated = synchronize_slots(wrconn);
        wait_for_slot_activity(some_slot_updated);
    }
}
```