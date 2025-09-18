# BackendMain

## Location
[src/backend/tcop/backend_startup.c:57-121](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tcop/backend_startup.c#L57-L121)

## Overview
BackendMain is the entry point for a new backend process that initializes the connection, reads the startup packet, authenticates the client, and starts the main processing loop.

## Definition


## Detailed Description
BackendMain serves as the main initialization function for PostgreSQL backend processes. It performs essential setup tasks including SSL reinitialization in EXEC_BACKEND builds, backend-specific initialization, shared memory setup, and finally transitions to the main PostgreSQL processing loop. The function ensures that all necessary infrastructure is in place before handing control over to PostgresMain for query processing.

## Parameters / Member Variables
- : Pointer to startup data cast to BackendStartupData structure containing initialization parameters
- : Size of the startup data, expected to match sizeof(BackendStartupData)

## Dependencies
- Functions called/Symbols referenced:
  - [BackendInitialize](BackendInitialize.md)
  - [secure_initialize](../s/secure_initialize.md) (SSL builds)
  - InitProcess
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