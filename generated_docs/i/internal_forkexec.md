# internal_forkexec

## Location
src/backend/postmaster/launch_backend.c: 294 - 403

## Overview
Creates a new PostgreSQL child process using fork+exec (Unix) or CreateProcess (Windows) in EXEC_BACKEND mode, with parameter passing through temporary files or shared memory.

## Definition
```c
static pid_t internal_forkexec(const char *child_kind, 
                              char *startup_data, size_t startup_data_len, 
                              ClientSocket *client_sock)
```

## Detailed Description
internal_forkexec implements the EXEC_BACKEND mechanism for spawning PostgreSQL child processes. This approach is mandatory on Windows and optional on Unix systems for testing. Unlike simple fork(), this method creates a completely new process that does not inherit the parent 's memory state, requiring explicit parameter passing and state restoration.

On Unix systems, it writes backend parameters to a temporary file, then fork+exec a new postgres process with `--forkchild=<child_kind>` and the parameter file path. On Windows, it uses CreateProcess() with shared memory for parameter passing.

The child process will start execution in SubPostmasterMain() which reads the parameters and restores the necessary state before calling the appropriate child main function.

## Parameters / Member Variables
- `child_kind`: String name of the child process type (e.g., "backend", "checkpointer", "bgwriter")
- `startup_data`: Optional initialization data specific to the child process type
- `startup_data_len`: Size of the startup_data buffer
- `client_sock`: Optional client socket information for backend processes

## Dependencies
- Functions called/Symbols referenced:
  - [save_backend_variables](../s/save_backend_variables.md) (to serialize state)
  - SizeOfBackendParameters (for memory allocation)
  - [fork_process](../f/fork_process.md) (Unix: for forking)
  - execv (Unix: to execute new process)
  - CreateProcess (Windows: to create new process)
  - AllocateFile/FreeFile (Unix: for temp file I/O)
  - CreateFileMapping/MapViewOfFile (Windows: for shared memory)
- Called from (representative examples):
  - [postmaster_child_launch](../p/postmaster_child_launch.md) (when EXEC_BACKEND is defined)

## Notes and Other Information
- This is a static function only available when EXEC_BACKEND is compiled in
- Has separate implementations for Unix (file-based parameter passing) and Windows (shared memory-based)
- On Unix, creates temporary files in PG_TEMP_FILES_DIR with unique names to avoid conflicts
- On Windows, creates a suspended process initially, then resumes after parameter setup
- Returns -1 on failure with appropriate error logging, or the child PID on success
- The child process must be able to read and interpret the parameters file/memory to restore state
- Located in src/backend/postmaster/launch_backend.c:294-403 (Unix) and similar range for Windows
- Critical for platforms where fork() is not available or reliable (primarily Windows)