# SubPostmasterMain

## Location
[src/backend/postmaster/launch_backend.c:581-695](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/postmaster/launch_backend.c#L581-L695)

## Overview
Entry point for PostgreSQL child processes launched via EXEC_BACKEND mode, responsible for restoring the child process to an equivalent state as if it had been created by simple fork().

## Definition
```c
void SubPostmasterMain(int argc, char *argv[])
```

## Detailed Description
SubPostmasterMain serves as the main entry point for child processes created through the EXEC_BACKEND mechanism (fork+exec on Unix, CreateProcess on Windows). Since these processes do not inherit the parent's memory state like traditional fork(), this function must recreate the necessary environment by reading serialized parameters and re-initializing subsystems.

The function expects specific command-line arguments: `--forkchild=<child_kind>` and a parameter file/handle path. It identifies the child process type, reads backend variables from the parameter source, re-attaches to shared memory if needed, reloads configuration and libraries, and finally calls the appropriate main function for the specific child process type.

This approach enables PostgreSQL to work on platforms where fork() is unavailable (Windows) or unreliable, and provides a testing mechanism for EXEC_BACKEND behavior on Unix systems.

## Parameters / Member Variables
- `argc`: Number of command-line arguments (expected to be 3)
- `argv`: Command-line argument array containing program name, `--forkchild=<child_kind>`, and parameter file/handle

## Dependencies
- Functions called/Symbols referenced:
  - [InitializeGUCOptions](../I/InitializeGUCOptions.md) (basic GUC setup)
  - [read_backend_variables](../r/read_backend_variables.md) (deserialize process parameters)
  - [ClosePostmasterPorts](../C/ClosePostmasterPorts.md) (close inherited sockets)
  - [InitPostmasterChild](../I/InitPostmasterChild.md) (child process initialization)
  - [PGSharedMemoryReAttach](../P/PGSharedMemoryReAttach.md)/PGSharedMemoryNoReAttach (shared memory handling)
  - [read_nondefault_variables](../r/read_nondefault_variables.md) (reload GUC configuration)
  - [checkDataDir](../c/checkDataDir.md) (validate data directory)
  - [LocalProcessControlFile](../L/LocalProcessControlFile.md) (read control file)
  - [process_shared_preload_libraries](../p/process_shared_preload_libraries.md) (reload preloaded libraries)
  - [InitShmemAccess](../I/InitShmemAccess.md) (restore shared memory pointers)
  - child_process_kinds (global process type array)
- Called from (representative examples):
  - [main](../m/main.md)() (when launched with --forkchild argument)

## Notes and Other Information
- Only used in EXEC_BACKEND builds (Windows and optional Unix testing)
- Sets IsPostmasterEnvironment=true and whereToSendOutput=DestNone for proper child environment
- Performs extensive validation of command-line arguments and child process type
- On Linux testing, may require `kernel.randomize_va_space=0` to ensure consistent memory mapping
- Child processes that don't need shared memory (like syslogger) are handled by PGSharedMemoryNoReAttach()
- The function never returns as it calls the child process main function which should be noreturn
- Located in src/backend/postmaster/launch_backend.c:581-695
- Critical for cross-platform compatibility and EXEC_BACKEND functionality

## Simplified Source

```c
void
SubPostmasterMain(int argc, char *argv[])
{
    char *startup_data;
    size_t startup_data_len;
    char *child_kind;
    BackendType child_type;
    bool found = false;

    // Configure environment for child process
    IsPostmasterEnvironment = true;
    whereToSendOutput = DestNone;

    // Initialize basic subsystems
    InitializeGUCOptions();

    // Validate command line arguments
    if (argc != 3)
        elog(FATAL, "invalid subpostmaster invocation");

    // Parse child process type from --forkchild=<name> argument
    if (strncmp(argv[1], "--forkchild=", 12) != 0)
        elog(FATAL, "invalid subpostmaster invocation (--forkchild argument missing)");

    child_kind = argv[1] + 12;

    // Find the child process type in the registry
    for (int idx = 0; idx < lengthof(child_process_kinds); idx++)
    {
        if (strcmp(child_process_kinds[idx].name, child_kind) == 0)
        {
            child_type = (BackendType) idx;
            found = true;
            break;
        }
    }
    if (!found)
        elog(ERROR, "unknown child kind %s", child_kind);

    // Read serialized backend variables from parameter file
    read_backend_variables(argv[2], &startup_data, &startup_data_len);

    // Close inherited postmaster sockets
    ClosePostmasterPorts(child_type == B_LOGGER);

    // Initialize child process environment
    InitPostmasterChild();

    // Re-attach to shared memory if needed for this process type
    if (child_process_kinds[child_type].shmem_attach)
        PGSharedMemoryReAttach();
    else
        PGSharedMemoryNoReAttach();

    // Restore GUC configuration
    read_nondefault_variables();

    // Validate data directory and set file permissions
    checkDataDir();

    // Read control file for configuration
    LocalProcessControlFile(false);

    // Reload preloaded libraries
    process_shared_preload_libraries();

    // Restore shared memory access
    if (UsedShmemSegAddr != NULL)
        InitShmemAccess(UsedShmemSegAddr);

    // Launch the specific child process main function
    child_process_kinds[child_type].main_fn(startup_data, startup_data_len);
    pg_unreachable(); // main_fn never returns
}
```