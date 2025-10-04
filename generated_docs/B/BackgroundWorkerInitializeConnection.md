# BackgroundWorkerInitializeConnection

## Location
[src/backend/postmaster/postmaster.c:4157-4190](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/postmaster/postmaster.c#L4157-L4190)

## Overview
Establishes a database connection for a background worker process, allowing it to access and operate on a specific database.

## Definition

```c
void
BackgroundWorkerInitializeConnection(const char *dbname, const char *username, uint32 flags)
```
## Detailed Description
This function initializes a database connection for a background worker process by calling InitPostgres() with appropriate parameters. It performs several validation checks to ensure the background worker is properly configured for database access and handles special connection flags that can bypass normal access restrictions.

The function validates that the background worker was registered with the BGWORKER_BACKEND_DATABASE_CONNECTION flag, indicating it requires database access. After establishing the connection, it transitions the process from initialization mode to normal processing mode.

## Parameters / Member Variables
- `dbname`: The name of the database to connect to
- `username`: The username/role to connect as
- `flags`: Connection flags that can modify connection behavior:
  - BGWORKER_BYPASS_ALLOWCONN: Ignore datallowconn restrictions
  - BGWORKER_BYPASS_ROLELOGINCHECK: Ignore rolcanlogin restrictions

## Dependencies
- Functions called/Symbols referenced:
  - [BackgroundWorker](BackgroundWorker.md) (struct type)
  - MyBgworkerEntry (global variable)
  - BGWORKER_BYPASS_ALLOWCONN (flag constant)
  - INIT_PG_OVERRIDE_ALLOW_CONNS (flag constant)
  - BGWORKER_BYPASS_ROLELOGINCHECK (flag constant)
  - INIT_PG_OVERRIDE_ROLE_LOGIN (flag constant)
  - BGWORKER_BACKEND_DATABASE_CONNECTION (flag constant)
  - [InitPostgres](../I/InitPostgres.md) (function)
  - IsInitProcessingMode (function)
  - SetProcessingMode (function)
  - NormalProcessing (processing mode constant)
- Called from (representative examples):
  - [ApplyLauncherMain](../A/ApplyLauncherMain.md) (src/backend/replication/logical/launcher.c:1154)
  - [worker_spi_main](../w/worker_spi_main.md) (src/test/modules/worker_spi/worker_spi.c:173)

## Notes and Other Information
- The function requires that the background worker was registered with BGWORKER_BACKEND_DATABASE_CONNECTION flag
- Uses InvalidOid for both database and role OIDs, relying on name-based lookup
- The init_flags parameter never honors session_preload_libraries for security reasons
- Validates that the process remains in initialization mode until explicitly transitioned to normal processing
- The function will terminate the process with FATAL error if connection requirements weren't indicated during registration
- Special bypass flags allow background workers to connect to databases or as roles that would normally be restricted

## Simplified Source

```c
void BackgroundWorkerInitializeConnection(const char *dbname, const char *username, uint32 flags) {
    BackgroundWorker *worker = MyBgworkerEntry;
    bits32 init_flags = 0;

    // Handle bypass flags for connection restrictions
    if (flags & BGWORKER_BYPASS_ALLOWCONN)
        init_flags |= INIT_PG_OVERRIDE_ALLOW_CONNS;
    if (flags & BGWORKER_BYPASS_ROLELOGINCHECK)
        init_flags |= INIT_PG_OVERRIDE_ROLE_LOGIN;

    // Verify worker was registered for database connections
    if (!(worker->bgw_flags & BGWORKER_BACKEND_DATABASE_CONNECTION))
        ereport(FATAL, (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
                       errmsg("database connection requirement not indicated during registration")));

    // Initialize database connection
    InitPostgres(dbname, InvalidOid, username, InvalidOid, init_flags, NULL);

    // Transition from init mode to normal processing
    if (!IsInitProcessingMode())
        ereport(ERROR, (errmsg("invalid processing mode in background worker")));
    SetProcessingMode(NormalProcessing);
}
```