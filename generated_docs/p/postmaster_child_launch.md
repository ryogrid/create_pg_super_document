# postmaster_child_launch

## Location
src/backend/postmaster/launch_backend.c: 231 - 293

## Overview
Launches a new PostgreSQL child process from the postmaster, handling both EXEC_BACKEND and traditional fork() modes to create properly initialized child processes.

## Definition
```c
pid_t postmaster_child_launch(BackendType child_type, 
                             char *startup_data, size_t startup_data_len, 
                             ClientSocket *client_sock)
```

## Detailed Description
postmaster_child_launch is the primary function for spawning new child processes from the PostgreSQL postmaster. It abstracts away the differences between EXEC_BACKEND mode (used on Windows and optionally on other platforms) and traditional Unix fork() mode.

In EXEC_BACKEND mode, it uses internal_forkexec() to create a new process via fork+exec, and the child will start execution in SubPostmasterMain(). In traditional mode, it uses fork_process() and performs immediate child process setup including closing unnecessary file descriptors, detaching from shared memory if not needed, setting up client socket information, and directly calling the appropriate main function for the child process type.

The function ensures that child processes are properly isolated from the postmaster while maintaining access to necessary shared resources like shared memory (when required by the process type).

## Parameters / Member Variables
- `child_type`: BackendType enumeration specifying what kind of child process to launch (e.g., B_BACKEND, B_CHECKPOINTER, B_BG_WRITER)
- `startup_data`: Optional contiguous data chunk passed to the child process for initialization
- `startup_data_len`: Size of the startup_data buffer
- `client_sock`: Optional client socket information, used primarily for backend processes handling client connections

## Dependencies
- Functions called/Symbols referenced:
  - [internal_forkexec](../i/internal_forkexec.md) (EXEC_BACKEND mode)
  - [fork_process](../f/fork_process.md) (traditional mode)
  - [ClosePostmasterPorts](../C/ClosePostmasterPorts.md)
  - [InitPostmasterChild](../I/InitPostmasterChild.md)
  - [dsm_detach_all](../d/dsm_detach_all.md)
  - [PGSharedMemoryDetach](../P/PGSharedMemoryDetach.md)
  - [MemoryContextSwitchTo](../M/MemoryContextSwitchTo.md)
  - child_process_kinds (global array)
- Called from (representative examples):
  - [BackendStartup](../B/BackendStartup.md) (for client backends)
  - [StartChildProcess](../S/StartChildProcess.md) (for auxiliary processes)
  - [do_start_bgworker](../d/do_start_bgworker.md) (for background workers)
  - [SysLogger_Start](../S/SysLogger_Start.md) (for syslogger process)

## Notes and Other Information
- Function asserts that it runs in postmaster environment only (IsPostmasterEnvironment && !IsUnderPostmaster)
- In traditional fork() mode, the child process path never returns (calls main_fn which should be noreturn)
- Child processes that do not need shared memory (like syslogger) are automatically detached from it
- The client socket parameter is copied into child process memory when provided
- Memory context is switched to TopMemoryContext before calling the child main function
- Located in src/backend/postmaster/launch_backend.c:231-282
- Returns the PID of the newly created child process to the parent (postmaster)