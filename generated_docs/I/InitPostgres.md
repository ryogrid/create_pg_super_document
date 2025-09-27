# InitPostgres

## Location
[src/backend/utils/init/postinit.c:738-1261](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/init/postinit.c#L738-L1261)

## Overview
InitPostgres performs comprehensive initialization of a PostgreSQL backend process, setting up database connections, user authentication, system catalogs, and all necessary infrastructure for normal database operations.

## Definition

```c
struct to the ProcArray.
	 *
	 * Once I have done this, I am visible to other backends!
	 */
	InitProcessPhase2();
```
## Detailed Description
InitPostgres is the main initialization function for PostgreSQL backend processes that handles the complete setup required for database operations. It performs authentication, establishes database connections, initializes system catalogs, sets up user sessions, and configures the runtime environment.

The function supports multiple initialization modes including bootstrap mode, standalone backends, autovacuum processes, background workers, and regular client connections. It handles both database and user specification by name or OID, with special logic for different process types.

The initialization process includes setting up shared memory structures, timeout handlers, transaction management, relation caches, authentication, database validation, permission checks, GUC parameter processing, and session state initialization. The function ensures proper sequencing of these operations to maintain system integrity and security.

## Parameters / Member Variables
- : Name of the database to connect to (NULL for OID-based specification)
- : OID of the database to connect to (InvalidOid for name-based specification)
- : Name of the role to connect as (NULL for OID-based specification)
- : OID of the role to connect as (InvalidOid for name-based specification)
- : Control flags including INIT_PG_LOAD_SESSION_LIBS, INIT_PG_OVERRIDE_ALLOW_CONNS, INIT_PG_OVERRIDE_ROLE_LOGIN
- : Optional output buffer for actual database name (must be NAMEDATALEN size if provided)

## Dependencies
- Functions called/Symbols referenced:
  - [InitProcessPhase2](InitProcessPhase2.md)
  - [SharedInvalBackendInit](../S/SharedInvalBackendInit.md)
  - [ProcSignalInit](../P/ProcSignalInit.md)
  - [RegisterTimeout](../R/RegisterTimeout.md) (various timeout types)
  - [CreateAuxProcessResourceOwner](../C/CreateAuxProcessResourceOwner.md)
  - [StartupXLOG](../S/StartupXLOG.md)
  - [RelationCacheInitialize](../R/RelationCacheInitialize.md)
  - [InitCatalogCache](InitCatalogCache.md)
  - [InitPlanCache](InitPlanCache.md)
  - [EnablePortalManager](../E/EnablePortalManager.md)
  - [pgstat_beinit](../p/pgstat_beinit.md)
  - [RelationCacheInitializePhase2](../R/RelationCacheInitializePhase2.md)
  - [SetCurrentStatementStartTimestamp](../S/SetCurrentStatementStartTimestamp.md)
  - [StartTransactionCommand](../S/StartTransactionCommand.md)
  - [GetTransactionSnapshot](../G/GetTransactionSnapshot.md)
  - [PerformAuthentication](../P/PerformAuthentication.md)
  - [InitializeSessionUserId](InitializeSessionUserId.md)
  - [InitializeSystemUser](InitializeSystemUser.md)
  - [GetDatabaseTuple](../G/GetDatabaseTuple.md)
  - [LockSharedObject](../L/LockSharedObject.md)
  - [CheckMyDatabase](../C/CheckMyDatabase.md)
  - [process_startup_options](../p/process_startup_options.md)
  - [process_settings](../p/process_settings.md)
  - [InitializeSearchPath](InitializeSearchPath.md)
  - [InitializeClientEncoding](InitializeClientEncoding.md)
  - [InitializeSession](InitializeSession.md)
- Called from (representative examples):
  - [BootstrapModeMain](../B/BootstrapModeMain.md)
  - [AutoVacWorkerMain](../A/AutoVacWorkerMain.md)
  - [BackgroundWorkerInitializeConnection](../B/BackgroundWorkerInitializeConnection.md)
  - [ReplSlotSyncWorkerMain](../R/ReplSlotSyncWorkerMain.md)
  - [PostgresMain](../P/PostgresMain.md)

## Notes and Other Information
- Must be called after BaseInit and InitProcess
- Function behavior varies significantly based on process type (bootstrap, standalone, autovacuum, background worker, regular backend)
- Performs critical security checks including authentication, connection limits, and privilege validation
- Establishes transaction context and acquires database locks to prevent concurrent drops
- Initializes system catalogs in multiple phases with careful dependency management
- Processes GUC parameters and applies database/role-specific settings
- Sets up shared memory visibility and process registration
- Critical function in PostgreSQL's startup sequence with complex error handling and cleanup logic
- The order of operations is extremely important and carefully designed to handle failure scenarios
- Special handling for WAL senders, autovacuum processes, and background workers

## Simplified Source

```c
// Simplified version of InitPostgres
void InitPostgres(const char *in_dbname, Oid dboid,
                  const char *username, Oid useroid,
                  bits32 flags, char *out_dbname) {
    bool bootstrap = IsBootstrapProcessingMode();
    bool am_superuser;
    char dbname[NAMEDATALEN];

    // Phase 1: Basic process setup
    InitProcessPhase2();                    // Add to ProcArray
    SharedInvalBackendInit(false);          // Shared invalidation setup
    ProcSignalInit();                       // Signal handling setup

    // Phase 2: Register timeout handlers (except in bootstrap mode)
    if (!bootstrap) {
        register_standard_timeouts();       // Deadlock, statement, lock timeouts
    }

    // Phase 3: XLOG setup for standalone processes
    if (!IsUnderPostmaster) {
        CreateAuxProcessResourceOwner();
        StartupXLOG();                      // Start transaction log
        setup_shutdown_callbacks();        // Register cleanup functions
    }

    // Phase 4: Initialize core systems
    RelationCacheInitialize();             // Relation cache hashtables
    InitCatalogCache();                    // System catalog caches
    InitPlanCache();                       // Query plan cache
    EnablePortalManager();                 // Portal management
    pgstat_beinit();                       // Statistics initialization
    RelationCacheInitializePhase2();       // Load shared catalogs

    // Early exit for autovacuum launcher
    if (AmAutoVacuumLauncherProcess()) {
        pgstat_bestart();
        return;
    }

    // Phase 5: Start transaction context
    if (!bootstrap) {
        SetCurrentStatementStartTimestamp();
        StartTransactionCommand();
        XactIsoLevel = XACT_READ_COMMITTED;  // Set isolation level
        GetTransactionSnapshot();            // Establish snapshot
    }

    // Phase 6: Authentication and user setup
    if (bootstrap || AmAutoVacuumWorkerProcess()) {
        InitializeSessionUserIdStandalone();
        am_superuser = true;
    } else if (AmBackgroundWorkerProcess()) {
        setup_background_worker_auth(username, useroid, flags);
        am_superuser = superuser();
    } else {
        // Normal client connection
        PerformAuthentication(MyProcPort);
        InitializeSessionUserId(username, useroid, false);
        am_superuser = superuser();
    }

    // Phase 7: Connection and privilege checks
    check_binary_upgrade_privileges(am_superuser);
    check_connection_limits(am_superuser);
    check_replication_permissions();

    // Early exit for physical WAL senders
    if (am_walsender && !am_db_walsender) {
        finalize_walsender_connection();
        return;
    }

    // Phase 8: Database identification and validation
    if (bootstrap) {
        dboid = Template1DbOid;
        MyDatabaseTableSpace = DEFAULTTABLESPACE_OID;
    } else {
        dboid = resolve_database_oid(in_dbname, dboid);
        if (!OidIsValid(dboid)) {
            // Background worker not bound to database
            pgstat_bestart();
            CommitTransactionCommand();
            return;
        }
    }

    // Phase 9: Database locking and verification
    if (!bootstrap) {
        LockSharedObject(DatabaseRelationId, dboid, 0, RowExclusiveLock);
        verify_database_exists(dboid, in_dbname, dbname, out_dbname);
    }

    // Phase 10: Set global database variables
    MyDatabaseId = dboid;
    MyProc->databaseId = MyDatabaseId;
    InvalidateCatalogSnapshot();

    // Phase 11: Database directory validation
    validate_database_directory();

    // Phase 12: Complete catalog initialization
    RelationCacheInitializePhase3();       // Load system catalogs
    initialize_acl();                      // ACL framework

    // Phase 13: Database-specific setup
    if (!bootstrap) {
        CheckMyDatabase(dbname, am_superuser,
                       (flags & INIT_PG_OVERRIDE_ALLOW_CONNS) != 0);
    }

    // Phase 14: Process startup options and settings
    if (MyProcPort != NULL) {
        process_startup_options(MyProcPort, am_superuser);
    }
    process_settings(MyDatabaseId, GetSessionUserId());

    // Apply authentication delay
    if (PostAuthDelay > 0) {
        pg_usleep(PostAuthDelay * 1000000L);
    }

    // Phase 15: Final session initialization
    InitializeSearchPath();                // Set schema search path
    InitializeClientEncoding();            // Character encoding
    InitializeSession();                   // Session state

    // Phase 16: Load session libraries
    if ((flags & INIT_PG_LOAD_SESSION_LIBS) != 0) {
        process_session_preload_libraries();
    }

    // Phase 17: Complete startup
    if (!bootstrap) {
        pgstat_bestart();                   // Report backend status
        CommitTransactionCommand();         // Commit startup transaction
    }
}
```

Key simplifications made:
- Consolidated multiple timeout registrations into single function call
- Abstracted complex authentication logic into helper functions
- Simplified database resolution and validation logic
- Removed detailed error handling and replaced with high-level checks
- Consolidated similar conditional branches
- Focused on the main execution path through the 17 phases
- Removed platform-specific and edge-case handling code
- Added phase comments to show the logical progression