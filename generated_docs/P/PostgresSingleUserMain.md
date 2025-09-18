# PostgresSingleUserMain

## Location
src/backend/tcop/postgres.c: 4129 - 4238

## Overview
Entry point function for PostgreSQL's single-user mode that performs initialization specific to standalone operation before delegating to PostgresMain for query processing.

## Definition


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
  - InitStandaloneProcess (initialize standalone process environment)
  - InitializeGUCOptions (set default GUC values)
  - process_postgres_switches (parse command-line options)
  - SelectConfigFiles (load configuration files)
  - checkDataDir/ChangeToDataDir (validate and access data directory)
  - CreateDataDirLockFile (create directory lock)
  - LocalProcessControlFile (read control file)
  - process_shared_preload_libraries (load preload libraries)
  - InitializeMaxBackends (set up backend limits)
  - process_shmem_requests (handle shared memory requests)
  - InitializeShmemGUCs (initialize shared memory dependent GUCs)
  - InitializeWalConsistencyChecking (initialize WAL consistency checking)
  - CreateSharedMemoryAndSemaphores (set up IPC)
  - InitProcess (create backend process structure)
  - PostgresMain (main query processing function)
- Called from (representative examples):
  - main (in src/backend/main/main.c:196)

## Notes and Other Information
- Only used in single-user mode (Assert(!IsUnderPostmaster) enforces this)
- Uses PGC_POSTMASTER context for configuration, giving it full privileges
- Defaults to username as database name if no database is specified in arguments
- Performs many of the same initialization steps as the postmaster but in a simplified, single-process context
- Critical for database recovery scenarios where normal server startup is not possible
- Sets up the complete PostgreSQL backend environment including shared memory, even though only one process will use it
- Records startup time similar to postmaster for consistency in timing operations