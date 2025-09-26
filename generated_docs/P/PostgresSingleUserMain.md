# PostgresSingleUserMain

## Location
[src/backend/tcop/postgres.c:4129-4238](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tcop/postgres.c#L4129-L4238)

## Overview
Entry point function for PostgreSQL's single-user mode that performs initialization specific to standalone operation before delegating to PostgresMain for query processing.

## Definition

```c
struct in shared memory. We must do this
	 * before we can use LWLocks.
	 */
	InitProcess();
```
## Detailed Description
PostgresSingleUserMain serves as the primary initialization function for PostgreSQL when running in single-user mode (standalone operation without a postmaster). This mode is typically used for database recovery, maintenance operations, or emergency database access when the server cannot start normally.

The function performs a comprehensive startup sequence that mirrors but simplifies the multi-user server startup process. It handles command-line argument parsing, configuration file loading, shared memory setup, and various system initializations required for database operations. The function ensures that all necessary infrastructure is in place before transferring control to PostgresMain for actual query processing.

Key responsibilities include:
- Processing command-line arguments and determining the target database
- Loading configuration files and setting up GUC parameters
- Validating and accessing the data directory
- Setting up shared memory and synchronization primitives
- Loading preload libraries and processing their requirements
- Initializing the process structure for the backend

## Parameters / Member Variables
- : Number of command-line arguments passed to the program
- : Array of command-line argument strings, where argv[0] is the program name
- : Username for the database session; used as default database name if no database is specified

## Dependencies
- Functions called/Symbols referenced:
  - [InitStandaloneProcess](../I/InitStandaloneProcess.md) (initialize standalone process environment)
  - [InitializeGUCOptions](../I/InitializeGUCOptions.md) (set default GUC values)
  - [process_postgres_switches](../p/process_postgres_switches.md) (parse command-line options)
  - [SelectConfigFiles](../S/SelectConfigFiles.md) (load configuration files)
  - [checkDataDir](../c/checkDataDir.md)/ChangeToDataDir (validate and access data directory)
  - [CreateDataDirLockFile](../C/CreateDataDirLockFile.md) (create directory lock)
  - [LocalProcessControlFile](../L/LocalProcessControlFile.md) (read control file)
  - [process_shared_preload_libraries](../p/process_shared_preload_libraries.md) (load preload libraries)
  - [InitializeMaxBackends](../I/InitializeMaxBackends.md) (set up backend limits)
  - [process_shmem_requests](../p/process_shmem_requests.md) (handle shared memory requests)
  - [InitializeShmemGUCs](../I/InitializeShmemGUCs.md) (initialize shared memory dependent GUCs)
  - [InitializeWalConsistencyChecking](../I/InitializeWalConsistencyChecking.md) (initialize WAL consistency checking)
  - [CreateSharedMemoryAndSemaphores](../C/CreateSharedMemoryAndSemaphores.md) (set up IPC)
  - [InitProcess](../I/InitProcess.md) (create backend process structure)
  - [PostgresMain](PostgresMain.md) (main query processing function)
- Called from (representative examples):
  - [main](../m/main.md) (in src/backend/main/main.c:196)

## Notes and Other Information
- Only used in single-user mode (Assert(!IsUnderPostmaster) enforces this)
- Uses PGC_POSTMASTER context for configuration, giving it full privileges
- Defaults to username as database name if no database is specified in arguments
- Performs many of the same initialization steps as the postmaster but in a simplified, single-process context
- Critical for database recovery scenarios where normal server startup is not possible
- Sets up the complete PostgreSQL backend environment including shared memory, even though only one process will use it
- Records startup time similar to postmaster for consistency in timing operations