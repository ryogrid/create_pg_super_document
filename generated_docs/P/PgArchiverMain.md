# PgArchiverMain

## Location
[src/backend/postmaster/pgarch.c:217-279](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/postmaster/pgarch.c#L217-L279)

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
  - [AuxiliaryProcessMainCommon](../A/AuxiliaryProcessMainCommon.md): Common initialization for auxiliary processes
  - [pqsignal](../p/pqsignal.md): Sets up signal handlers
  - [SignalHandlerForConfigReload](../S/SignalHandlerForConfigReload.md): Handler for SIGHUP configuration reload
  - [SignalHandlerForShutdownRequest](../S/SignalHandlerForShutdownRequest.md): Handler for SIGTERM shutdown
  - [procsignal_sigusr1_handler](../p/procsignal_sigusr1_handler.md): Handler for SIGUSR1 signals
  - [pgarch_waken_stop](../p/pgarch_waken_stop.md): Handler for SIGUSR2 archiver wake/stop signals
  - XLogArchivingActive: Checks if WAL archiving is enabled
  - [on_shmem_exit](../o/on_shmem_exit.md): Registers cleanup function
  - [binaryheap_allocate](../b/binaryheap_allocate.md): Creates priority heap for file management
  - AllocSetContextCreate: Creates memory context
  - [LoadArchiveLibrary](../L/LoadArchiveLibrary.md): Loads configured archive library
  - [pgarch_MainLoop](../p/pgarch_MainLoop.md): Main processing loop
  - [pgarch_die](../p/pgarch_die.md): Cleanup function for process exit
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

## Simplified Source

```c
void PgArchiverMain(char *startup_data, size_t startup_data_len) {
    Assert(startup_data_len == 0);

    // Set process type and perform common auxiliary process initialization
    MyBackendType = B_ARCHIVER;
    AuxiliaryProcessMainCommon();

    // Configure signal handlers
    pqsignal(SIGHUP, SignalHandlerForConfigReload);
    pqsignal(SIGINT, SIG_IGN);
    pqsignal(SIGTERM, SignalHandlerForShutdownRequest);
    pqsignal(SIGALRM, SIG_IGN);
    pqsignal(SIGPIPE, SIG_IGN);
    pqsignal(SIGUSR1, procsignal_sigusr1_handler);
    pqsignal(SIGUSR2, pgarch_waken_stop);
    pqsignal(SIGCHLD, SIG_DFL);

    // Unblock signals for processing
    sigprocmask(SIG_SETMASK, &UnBlockSig, NULL);

    // Verify archiving is enabled
    Assert(XLogArchivingActive());

    // Register cleanup function for process exit
    on_shmem_exit(pgarch_die, 0);

    // Advertise process number for backend communication
    PgArch->pgprocno = MyProcNumber;

    // Initialize file management workspace
    arch_files = palloc(sizeof(struct arch_files_state));
    arch_files->arch_files_size = 0;

    // Create priority heap for file archiving order
    arch_files->arch_heap = binaryheap_allocate(NUM_FILES_PER_DIRECTORY_SCAN,
                                               ready_file_comparator, NULL);

    // Create archiver memory context
    archive_context = AllocSetContextCreate(TopMemoryContext,
                                           "archiver",
                                           ALLOCSET_DEFAULT_SIZES);

    // Load the configured archive library
    LoadArchiveLibrary();

    // Enter main processing loop
    pgarch_MainLoop();

    // Exit when main loop completes
    proc_exit(0);
}
```