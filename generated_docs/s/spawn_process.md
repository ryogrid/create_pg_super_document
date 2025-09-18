# spawn_process

## Location
src/test/regress/pg_regress.c: 1199 - 1260

## Overview
Creates a child process to execute a shell command asynchronously, returning the process identifier for later monitoring without waiting for completion.

## Definition
```c
PID_TYPE spawn_process(const char *cmdline)
```

## Detailed Description
This function creates a new process to execute a shell command without blocking the parent process. It provides platform-specific implementations for Unix-like systems (using fork/exec) and Windows (using CreateProcess). The function is designed for parallel test execution, allowing multiple test processes to run concurrently.

On Unix systems, the function uses fork() to create a child process, then uses execl() to execute the shell command via the configured shell program. To optimize process management, it prefixes the command with "exec" to replace the shell process rather than creating a subprocess chain, reducing the total number of processes per parallel test.

On Windows, it uses CreateRestrictedProcess() to spawn the command via CMD.EXE (or the COMSPEC environment variable), properly formatting the command line and managing Windows process handles.

## Parameters / Member Variables
- `cmdline`: Shell command string to execute in the spawned process

## Dependencies
- Functions called/Symbols referenced:
  - fflush (Unix)
  - pg_disable_aslr (Unix, if EXEC_BACKEND defined)
  - fork (Unix)
  - bail (Unix)
  - psprintf (Unix)
  - execl (Unix) 
  - bail_noatexit (Unix)
  - getenv (Windows)
  - memset (Windows)
  - CreateRestrictedProcess (Windows)
  - CloseHandle (Windows)
  - shellprog (global variable, Unix)
- Called from (representative examples):
  - regression_main
  - psql_start_test
  - ecpg_start_test
  - isolation_start_test

## Notes and Other Information
- Returns PID_TYPE which is pid_t on Unix systems and HANDLE on Windows
- On Unix, flushes all I/O buffers before forking to prevent duplicate output
- Uses pg_disable_aslr() on systems with EXEC_BACKEND to ensure consistent memory layout
- On Unix, uses bail_noatexit() in child process to avoid atexit handlers that could interfere with the parent
- On Windows, finds the command processor via COMSPEC environment variable, defaulting to "CMD"
- Windows implementation closes the thread handle immediately since only the process handle is needed for monitoring
- The function is specifically optimized for parallel test execution scenarios
- Process cleanup and waiting must be handled separately by the caller