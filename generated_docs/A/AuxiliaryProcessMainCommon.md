# AuxiliaryProcessMainCommon

## Location
[src/backend/postmaster/auxprocess.c:44-100](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/postmaster/auxprocess.c#L44-L100)

## Overview
Common initialization code for auxiliary processes such as the background writer, WAL writer, WAL receiver, and startup process, providing essential setup without full InitPostgres initialization.

## Definition
void AuxiliaryProcessMainCommon(void)

## Detailed Description
AuxiliaryProcessMainCommon performs the common initialization sequence required by all auxiliary processes in PostgreSQL. Unlike regular backend processes that go through the full InitPostgres initialization, auxiliary processes have a more streamlined startup sequence that focuses on essential services like shared memory access, LWLocks, and process identification without transaction processing capabilities.

The function sets up the minimal infrastructure needed for auxiliary processes to function within the PostgreSQL shared memory environment, including process registration, signal handling, resource management, and statistics collection. It also registers a shutdown callback to ensure proper cleanup when the process terminates.

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - [MemoryContextDelete](../M/MemoryContextDelete.md)
  - init_ps_display
  - SetProcessingMode (with BootstrapProcessing and NormalProcessing)
  - [InitAuxiliaryProcess](../I/InitAuxiliaryProcess.md)
  - [BaseInit](../B/BaseInit.md)
  - [ProcSignalInit](../P/ProcSignalInit.md)
  - [CreateAuxProcessResourceOwner](../C/CreateAuxProcessResourceOwner.md)
  - [pgstat_beinit](../p/pgstat_beinit.md)
  - [pgstat_bestart](../p/pgstat_bestart.md)
  - [before_shmem_exit](../b/before_shmem_exit.md)
  - [ShutdownAuxiliaryProcess](../S/ShutdownAuxiliaryProcess.md)

- Called from:
  - [BackgroundWriterMain](../B/BackgroundWriterMain.md)
  - [CheckpointerMain](../C/CheckpointerMain.md)
  - [PgArchiverMain](../P/PgArchiverMain.md)
  - [StartupProcessMain](../S/StartupProcessMain.md)
  - [WalSummarizerMain](../W/WalSummarizerMain.md)
  - [WalWriterMain](../W/WalWriterMain.md)
  - [WalReceiverMain](../W/WalReceiverMain.md)

## Notes and Other Information
- Must be called under the postmaster (Assert(IsUnderPostmaster))
- Releases the postmaster's working memory context to prevent memory leaks
- Sets IgnoreSystemIndexes to true, appropriate for auxiliary processes
- Creates a PGPROC entry for LWLock usage and shared memory access
- Establishes a resource owner for managing buffer pins outside transactions
- Registers ShutdownAuxiliaryProcess as a before-shutdown callback for proper cleanup
- Transitions from BootstrapProcessing to NormalProcessing mode during initialization

## Simplified Source

```c
// Simplified version of AuxiliaryProcessMainCommon
void AuxiliaryProcessMainCommon(void) {
    // Step 1: Verify we're running under postmaster
    Assert(IsUnderPostmaster);

    // Step 2: Clean up inherited memory context
    if (PostmasterContext) {
        MemoryContextDelete(PostmasterContext);
        PostmasterContext = NULL;
    }

    // Step 3: Set up process display and bootstrap mode
    init_ps_display(NULL);
    SetProcessingMode(BootstrapProcessing);
    IgnoreSystemIndexes = true;

    // Step 4: Initialize core process infrastructure
    InitAuxiliaryProcess();  // Create PGPROC for LWLocks and shared memory
    BaseInit();              // Basic backend initialization
    ProcSignalInit();        // Signal handling setup

    // Step 5: Set up resource management
    CreateAuxProcessResourceOwner();  // For buffer pins outside transactions

    // Step 6: Initialize statistics collection
    pgstat_beinit();
    pgstat_bestart();

    // Step 7: Register cleanup callback and switch to normal mode
    before_shmem_exit(ShutdownAuxiliaryProcess, 0);
    SetProcessingMode(NormalProcessing);
}
```

Key simplifications made:
- Added step-by-step comments explaining the initialization sequence
- Grouped related operations logically
- Removed detailed explanatory comments for brevity
- Preserved the essential flow and all function calls
- Maintained the correct order of operations