# BackendMain

## Location
[src/backend/tcop/backend_startup.c:57-121](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tcop/backend_startup.c#L57-L121)

## Overview
BackendMain is the entry point for a new backend process that initializes the connection, reads the startup packet, authenticates the client, and starts the main processing loop.

## Definition

```c
structures contain function pointers and cannot be passed through the
	 * parameter file.
	 *
	 * If for some reason reload fails (maybe the user installed broken key
	 * files), soldier on without SSL;
```
## Detailed Description
BackendMain serves as the main initialization function for PostgreSQL backend processes. It performs essential setup tasks including SSL reinitialization in EXEC_BACKEND builds, backend-specific initialization, shared memory setup, and finally transitions to the main PostgreSQL processing loop. The function ensures that all necessary infrastructure is in place before handing control over to PostgresMain for query processing.

## Parameters / Member Variables
- : Pointer to startup data cast to BackendStartupData structure containing initialization parameters
- : Size of the startup data, expected to match sizeof(BackendStartupData)

## Dependencies
- Functions called/Symbols referenced:
  - [BackendInitialize](BackendInitialize.md)
  - [secure_initialize](../s/secure_initialize.md) (SSL builds)
  - [InitProcess](../I/InitProcess.md)
  - [PostgresMain](../P/PostgresMain.md)
  - [MemoryContextSwitchTo](../M/MemoryContextSwitchTo.md)
- Called from (representative examples):
  - child_process_kind (in launch_backend.c)

## Notes and Other Information
- Function assumes MyClientSocket is already initialized and not NULL
- In EXEC_BACKEND builds, SSL library context must be reinitialized as function pointers cannot be passed through parameter files
- SSL initialization failure is logged but non-fatal - the backend continues without SSL rather than failing completely
- Creates a per-backend PGPROC struct in shared memory before accessing shared resources
- Switches from PostmasterContext to TopMemoryContext before calling PostgresMain
- Located in src/backend/tcop/backend_startup.c:57-121

## Simplified Source

```c
// Simplified version of BackendMain
void BackendMain(char *startup_data, size_t startup_data_len) {
    BackendStartupData *bsdata = (BackendStartupData *) startup_data;

    // Validate input parameters
    Assert(startup_data_len == sizeof(BackendStartupData));
    Assert(MyClientSocket != NULL);

    // SSL reinitialization for EXEC_BACKEND builds
    #ifdef EXEC_BACKEND
    #ifdef USE_SSL
    if (EnableSSL) {
        if (secure_initialize(false) == 0) {
            LoadedSSL = true;
        } else {
            // Log SSL failure but continue without SSL
            ereport(LOG, (errmsg("SSL configuration could not be loaded in child process")));
        }
    }
    #endif
    #endif

    // Initialize backend and collect startup packet
    BackendInitialize(MyClientSocket, bsdata->canAcceptConnections);

    // Create per-backend PGPROC struct in shared memory
    InitProcess();

    // Switch to proper memory context
    MemoryContextSwitchTo(TopMemoryContext);

    // Start main PostgreSQL processing loop
    PostgresMain(MyProcPort->database_name, MyProcPort->user_name);
}
```

Key simplifications made:
- Removed detailed SSL-related comments for clarity
- Consolidated conditional compilation blocks
- Added high-level comments explaining each major step
- Focused on the main execution path
- Maintained the essential initialization sequence