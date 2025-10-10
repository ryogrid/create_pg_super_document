# HandlePgArchInterrupts

## Location
[src/backend/postmaster/pgarch.c:859-910](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/postmaster/pgarch.c#L859-L910)

## Overview
A static interrupt handler function that processes various system signals and administrative requests for the PostgreSQL WAL archiver process during its main execution loops.

## Definition
```c
static void HandlePgArchInterrupts(void)
```

## Detailed Description
`HandlePgArchInterrupts` serves as the central interrupt processing hub for the PostgreSQL archiver process. It is designed to be called periodically from the main archiver loops (`pgarch_MainLoop` and `pgarch_ArchiverCopyLoop`) to handle various asynchronous events and administrative requests.

The function processes several types of interrupts and system events:

1. **Process Signal Barriers**: Handles synchronization barriers that coordinate actions across multiple PostgreSQL processes.

2. **Memory Context Logging**: Processes requests to log memory context information for debugging and monitoring purposes.

3. **Configuration Reloads**: Handles SIGHUP signals that trigger configuration file reloading, with special handling for archive library changes.

The function includes sophisticated logic for handling archive library changes, which requires a complete archiver process restart due to the inability to dynamically unload shared libraries in PostgreSQL.

## Parameters / Member Variables



## Dependencies
- Functions called/Symbols referenced:
  - [ProcessProcSignalBarrier](../P/ProcessProcSignalBarrier.md): Handles process synchronization barriers
  - [ProcessLogMemoryContextInterrupt](../P/ProcessLogMemoryContextInterrupt.md): Processes memory context logging requests  
  - `ProcessConfigFile`: Reloads configuration file with PGC_SIGHUP context
  - [proc_exit](../p/proc_exit.md): Terminates the current process cleanly
  - [pstrdup](../p/pstrdup.md): Duplicates a string in PostgreSQL memory context
  - [pfree](../p/pfree.md): Frees memory allocated in PostgreSQL memory context
  - `ereport`: PostgreSQL logging and error reporting mechanism

- Called from (representative examples):
  - [pgarch_MainLoop](../p/pgarch_MainLoop.md): Called periodically during the main archiver loop
  - [pgarch_ArchiverCopyLoop](../p/pgarch_ArchiverCopyLoop.md): Called during file copying operations

## Notes and Other Information  
- This is a static function, only accessible within the pgarch.c source file
- The function explicitly does NOT handle shutdown requests - those are handled differently by each calling loop
- Configuration validation ensures that `archive_command` and `archive_library` are not both set simultaneously
- [Archive](../A/Archive.md) library changes trigger a complete process restart due to PostgreSQLs inability to unload shared libraries
- The function provides graceful handling of various system administrative tasks without disrupting normal archival operations
- Memory management is careful to avoid leaks, using `pstrdup`/`pfree` for temporary string storage
- Error handling includes both ERROR level (fatal) and LOG level (informational) messages as appropriate
- The restart mechanism for archive library changes ensures proper cleanup through the modules shutdown callback

## Simplified Source

```c
static void
HandlePgArchInterrupts(void)
{
    // Handle process synchronization barriers
    if (ProcSignalBarrierPending)
        ProcessProcSignalBarrier();

    // Handle memory context logging requests
    if (LogMemoryContextPending)
        ProcessLogMemoryContextInterrupt();

    // Handle configuration reload (SIGHUP)
    if (ConfigReloadPending)
    {
        char *old_archive_lib = pstrdup(XLogArchiveLibrary);

        ConfigReloadPending = false;
        ProcessConfigFile(PGC_SIGHUP);

        // Validate that both archive_command and archive_library aren't set
        if (XLogArchiveLibrary[0] != '\0' && XLogArchiveCommand[0] != '\0')
            ereport(ERROR, "both archive_command and archive_library set");

        // Check if archive library changed
        bool lib_changed = strcmp(XLogArchiveLibrary, old_archive_lib) != 0;
        pfree(old_archive_lib);

        if (lib_changed)
        {
            // Restart archiver process to load new library
            // (can't dynamically unload libraries in PostgreSQL)
            ereport(LOG, "restarting archiver process due to archive_library change");
            proc_exit(0);
        }
    }
}
```