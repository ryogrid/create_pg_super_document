# kill_bgchild_atexit

## Location
[src/bin/pg_basebackup/pg_basebackup.c:308-319](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_basebackup/pg_basebackup.c#L308-L319)

## Overview
An exit handler function that terminates background child processes during pg_basebackup cleanup to prevent orphaned processes.

## Definition


## Detailed Description
This function serves as an atexit() handler specifically designed for Unix-like systems to ensure that background child processes (such as WAL streaming processes) are properly terminated when pg_basebackup exits. The function prevents orphaned child processes that might continue running and attempting to stream data after the parent process has terminated.

On Windows systems, background threads automatically terminate with the parent process, so this cleanup is not necessary. However, on Unix systems, subprocess termination must be explicitly handled to prevent resource leaks and zombie processes.

The function only sends a SIGTERM signal if the background child process is still running (bgchild_exited is false), providing a graceful termination approach.

## Parameters / Member Variables
This function takes no parameters but operates on global variables:
- : Process ID (pid_t) of the background child process
- : Boolean flag indicating whether the child process has already exited

## Dependencies
- Functions called/Symbols referenced:
  - kill (system call to send signals to processes)
- Called from (representative examples):
  - [StartLogStreamer](../S/StartLogStreamer.md) (in pg_basebackup.c via atexit registration)

## Notes and Other Information
- This is a static function with internal linkage within pg_basebackup.c
- Only relevant for Unix-like systems; Windows uses background threads that terminate automatically
- Uses SIGTERM for graceful termination rather than SIGKILL for forced termination
- Part of the resource cleanup mechanism in pg_basebackup
- Prevents orphaned WAL streaming or compression processes
- The function checks both that a valid process ID exists (bgchild > 0) and that the process hasn't already exited
- Registered with atexit() when background processes are started, typically in StartLogStreamer()