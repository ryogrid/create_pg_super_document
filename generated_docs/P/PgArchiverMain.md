# PgArchiverMain

## Location
src/backend/postmaster/pgarch.c: 217 - 279

## Overview
PgArchiverMain is the main entry point and initialization function for the PostgreSQL archiver process, responsible for setting up the archiver environment and starting its main processing loop.

## Definition
```c
void PgArchiverMain(char *startup_data, size_t startup_data_len)
```

## Detailed Description
This function serves as the main entry point for the archiver process after it has been forked by the postmaster. It performs comprehensive initialization including signal handler setup, memory context creation, workspace allocation, and archiver library loading. The function configures the process to handle specific signals while ignoring others, sets up shared memory connections, creates data structures for managing archive files, and loads the configured archive library before entering the main processing loop.

## Parameters / Member Variables
- `startup_data`: Character array containing startup data (currently unused, expected to be NULL)
- `startup_data_len`: Size of startup data (expected to be 0)

## Dependencies
- Functions called/Symbols referenced:
  - AuxiliaryProcessMainCommon: Common initialization for auxiliary processes
  - pqsignal: Sets up signal handlers
  - SignalHandlerForConfigReload: Handler for SIGHUP configuration reload
  - SignalHandlerForShutdownRequest: Handler for SIGTERM shutdown
  - procsignal_sigusr1_handler: Handler for SIGUSR1 signals
  - pgarch_waken_stop: Handler for SIGUSR2 archiver wake/stop signals
  - XLogArchivingActive: Checks if WAL archiving is enabled
  - on_shmem_exit: Registers cleanup function
  - binaryheap_allocate: Creates priority heap for file management
  - AllocSetContextCreate: Creates memory context
  - LoadArchiveLibrary: Loads configured archive library
  - pgarch_MainLoop: Main processing loop
  - pgarch_die: Cleanup function for process exit
- Called from (representative examples):
  - child_process_kind: Process launcher infrastructure

## Notes and Other Information
- Sets MyBackendType to B_ARCHIVER to identify process type
- Ignores SIGINT, SIGALRM, and SIGPIPE signals
- Uses SIGUSR2 for waking/stopping the archiver
- Creates a binary heap for prioritizing files to archive
- Establishes "archiver" memory context for allocation management
- Validates that XLog archiving is active before proceeding
- Advertises process number in shared memory for backend communication
- Exits with status 0 when main loop completes
- Part of PostgreSQL's auxiliary process infrastructure