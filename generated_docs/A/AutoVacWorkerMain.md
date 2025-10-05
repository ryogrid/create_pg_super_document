# AutoVacWorkerMain

## Location
[src/backend/postmaster/autovacuum.c:1359-1588](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/postmaster/autovacuum.c#L1359-L1588)

## Overview
The main entry point for autovacuum worker processes that initializes the worker environment, connects to a database, and performs autovacuum operations.

## Definition

```c
struct in shared memory.  We must do this
	 * before we can use LWLocks or access any shared memory.
	 */
	InitProcess();
```
## Detailed Description
The  function serves as the complete lifecycle manager for autovacuum worker processes. It handles the entire initialization sequence, database connection, and cleanup for worker processes spawned by the postmaster at the request of the autovacuum launcher.

The function performs several critical initialization steps:

1. **Process Setup**: Establishes the process type as B_AUTOVAC_WORKER, releases postmaster memory context, and sets up signal handlers for proper worker operation including SIGINT for vacuum cancellation and SIGTERM for clean shutdown.

2. **Security Configuration**: Applies security-hardened configuration settings including an empty search_path to prevent malicious code execution, disabling zero_damaged_pages, and forcing timeouts to zero to prevent maintenance interruption.

3. **Transaction Isolation**: Forces READ COMMITTED isolation level to minimize overhead and avoid deadlocks, and disables synchronous replication for anti-wraparound maintenance.

4. **Worker Registration**: Retrieves worker information from shared memory, registers itself in the running workers list, and notifies the launcher of successful startup.

5. **Database Connection**: Connects to the assigned database using InitPostgres with override flags to ignore datallowconn restrictions, reports the connection to pgstat, and sets up the process display.

6. **Vacuum Execution**: Calls do_autovacuum() to perform the actual vacuum and analyze operations on the selected database.

The function includes comprehensive error handling with sigsetjmp/longjmp to ensure clean exit on any errors, and uses proc_exit(0) for normal termination which triggers cleanup callbacks.

## Parameters / Member Variables
- : Startup data passed from postmaster (expected to be NULL/empty for autovacuum workers)
- : Length of startup data (expected to be 0 for autovacuum workers)

## Dependencies
- Functions called/Symbols referenced:
  -  (release postmaster context)
  - / (process display management)
  -  (processing mode transitions)
  -  (signal handler registration)
  - / (process and backend initialization)
  -  (security and performance configuration)
  - / (worker information access)
  -  (worker list management)
  -  (cleanup callback registration)
  -  (statistics reporting)
  -  (database connection with overrides)
  - / (transaction ID management)
  -  (actual vacuum operations)
  -  (process termination)

- Called from (representative examples):
  - child_process_kind dispatch mechanism (src/backend/postmaster/launch_backend.c:189)

## Notes and Other Information
- Implements comprehensive security hardening by forcing safe configuration values
- Uses sigsetjmp/longjmp for robust error handling and clean exit on failures
- Handles the case where no worker slot is available (race condition) by logging and exiting gracefully
- Reports autovac startup to pgstat before database connection to update last_autovac_time even on connection failures
- Designed to prevent "stuck" autovacuum scenarios on unopenable databases
- Critical for maintaining database health through automated vacuum and analyze operations
- Integrates with PostgreSQL's process management and shared memory infrastructure
- The worker notifies the launcher of successful startup via SIGUSR2 signal

## Simplified Source

```c
void AutoVacWorkerMain(char *startup_data, size_t startup_data_len)
{
    sigjmp_buf local_sigjmp_buf;
    Oid dbid;

    // Clean up postmaster context and initialize worker
    if (PostmasterContext) {
        MemoryContextDelete(PostmasterContext);
        PostmasterContext = NULL;
    }

    MyBackendType = B_AUTOVAC_WORKER;
    init_ps_display(NULL);
    SetProcessingMode(InitProcessing);

    // Set up signal handlers for worker lifecycle
    pqsignal(SIGHUP, SignalHandlerForConfigReload);
    pqsignal(SIGINT, StatementCancelHandler);  // Cancel current vacuum
    pqsignal(SIGTERM, die);                    // Clean shutdown
    pqsignal(SIGUSR1, procsignal_sigusr1_handler);

    InitProcess();
    BaseInit();

    // Error handling: clean exit on any errors
    if (sigsetjmp(local_sigjmp_buf, 1) != 0) {
        error_context_stack = NULL;
        HOLD_INTERRUPTS();
        EmitErrorReport();
        proc_exit(0);
    }
    PG_exception_stack = &local_sigjmp_buf;

    // Apply security-hardened configuration
    SetConfigOption("search_path", "", PGC_SUSET, PGC_S_OVERRIDE);
    SetConfigOption("zero_damaged_pages", "false", PGC_SUSET, PGC_S_OVERRIDE);
    SetConfigOption("statement_timeout", "0", PGC_SUSET, PGC_S_OVERRIDE);
    SetConfigOption("default_transaction_isolation", "read committed", PGC_SUSET, PGC_S_OVERRIDE);
    SetConfigOption("stats_fetch_consistency", "none", PGC_SUSET, PGC_S_OVERRIDE);

    // Get assigned database from shared memory
    LWLockAcquire(AutovacuumLock, LW_EXCLUSIVE);
    if (AutoVacuumShmem->av_startingWorker != NULL) {
        MyWorkerInfo = AutoVacuumShmem->av_startingWorker;
        dbid = MyWorkerInfo->wi_dboid;
        MyWorkerInfo->wi_proc = MyProc;

        // Register in running workers list
        dlist_push_head(&AutoVacuumShmem->av_runningWorkers, &MyWorkerInfo->wi_links);
        AutoVacuumShmem->av_startingWorker = NULL;
        LWLockRelease(AutovacuumLock);

        on_shmem_exit(FreeWorkerInfo, 0);

        // Notify launcher of successful startup
        if (AutoVacuumShmem->av_launcherpid != 0)
            kill(AutoVacuumShmem->av_launcherpid, SIGUSR2);
    } else {
        elog(WARNING, "autovacuum worker started without a worker entry");
        dbid = InvalidOid;
        LWLockRelease(AutovacuumLock);
    }

    // Connect to database and perform vacuum work
    if (OidIsValid(dbid)) {
        char dbname[NAMEDATALEN];

        // Report startup to stats (even if connection fails)
        pgstat_report_autovac(dbid);

        // Connect to assigned database
        InitPostgres(NULL, dbid, NULL, InvalidOid, INIT_PG_OVERRIDE_ALLOW_CONNS, dbname);
        SetProcessingMode(NormalProcessing);
        set_ps_display(dbname);

        ereport(DEBUG1, (errmsg_internal("autovacuum: processing database \"%s\"", dbname)));

        // Perform autovacuum operations
        recentXid = ReadNextTransactionId();
        recentMulti = ReadNextMultiXactId();
        do_autovacuum();
    }

    // Exit cleanly
    proc_exit(0);
}
```