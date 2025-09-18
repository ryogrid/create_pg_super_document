# start_postmaster

## Location
src/bin/pg_ctl/pg_ctl.c: 439 - 591

## Overview
Starts the PostgreSQL postmaster process and returns its process ID (PID), handling platform-specific differences between Unix and Windows systems.

## Definition


## Detailed Description
The  function is responsible for launching the PostgreSQL postmaster process. It implements different strategies for Unix and Windows platforms:

**Unix Implementation:**
- Uses  to create a child process
- In the child process, calls  to detach from the launching process group
- Executes the postmaster via shell () to handle command-line quoting and redirection
- Returns the actual postmaster PID to the parent process

**Windows Implementation:**
- Uses  to launch the postmaster via CMD.EXE
- Handles log file permissions carefully to avoid privilege escalation issues
- Returns the shell process PID (not the actual postmaster PID)
- Stores a handle to the shell process in  for later use

The function constructs appropriate command lines with data directory options, additional postmaster options, and output redirection based on whether a log file is specified.

## Parameters / Member Variables
This function takes no parameters but relies on several global variables:
- : Path to the postmaster executable
- : PostgreSQL data directory option string  
- : Additional postmaster command-line options
- : Optional log file path for output redirection
- : (Windows only) Handle to the launched shell process

## Dependencies
- Functions called/Symbols referenced:
  -  (Unix)
  -  (Unix)
  -  (Unix) 
  -  (Windows)
  - 
  - 
  -  (when EXEC_BACKEND defined)
  - ,  (Windows log file handling)
- Called from (representative examples):
  -  in pg_ctl.c
  - Various functions in pg_upgrade

## Notes and Other Information
- On Windows, the returned PID is for the shell process, not the postmaster itself, but is still useful for process existence checks
- The function handles output redirection to log files or suppresses output by redirecting to DEVNULL
- Uses shell execution to properly handle command-line quoting and special characters
- Includes error handling for fork failures, exec failures, and Windows process creation failures
- The function exits with status 1 on any critical failure rather than returning an error code