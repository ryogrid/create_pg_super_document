# SubPostmasterMain

## Location
src/backend/postmaster/launch_backend.c: 581 - 695

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
  - InitializeGUCOptions (basic GUC setup)
  - read_backend_variables (deserialize process parameters)
  - ClosePostmasterPorts (close inherited sockets)
  - InitPostmasterChild (child process initialization)
  - PGSharedMemoryReAttach/PGSharedMemoryNoReAttach (shared memory handling)
  - read_nondefault_variables (reload GUC configuration)
  - checkDataDir (validate data directory)
  - LocalProcessControlFile (read control file)
  - process_shared_preload_libraries (reload preloaded libraries)
  - InitShmemAccess (restore shared memory pointers)
  - child_process_kinds (global process type array)
- Called from (representative examples):
  - main() (when launched with --forkchild argument)

## Notes and Other Information
- Only used in EXEC_BACKEND builds (Windows and optional Unix testing)
- Sets IsPostmasterEnvironment=true and whereToSendOutput=DestNone for proper child environment
- Performs extensive validation of command-line arguments and child process type
- On Linux testing, may require `kernel.randomize_va_space=0` to ensure consistent memory mapping
- Child processes that don't need shared memory (like syslogger) are handled by PGSharedMemoryNoReAttach()
- The function never returns as it calls the child process main function which should be noreturn
- Located in src/backend/postmaster/launch_backend.c:581-695
- Critical for cross-platform compatibility and EXEC_BACKEND functionality