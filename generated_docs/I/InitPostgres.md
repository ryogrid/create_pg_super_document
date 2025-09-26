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