# SysLogger_Start

## Location
src/backend/postmaster/syslogger.c: 595 - 801

## Overview
SysLogger_Start is a postmaster subroutine that initializes and launches the system logger subprocess, setting up the logging infrastructure including pipes, log files, and stderr redirection.

## Definition


## Detailed Description
SysLogger_Start implements the initialization and startup sequence for PostgreSQL's logging collector process. This function is called by the postmaster to establish the logging infrastructure and create a dedicated logger process. Key responsibilities include:

1. **Pipe Creation**: Creates the communication pipe between postmaster/backends and the logger process (handles both Unix pipe() and Windows CreatePipe)
2. **Log Directory Setup**: Ensures the log directory exists and is writable
3. **Initial Log File Creation**: Opens the initial log files (stderr, CSV, JSON) to verify write permissions before launching the logger process
4. **Process Launch**: Forks/creates the syslogger child process using postmaster_child_launch
5. **Stderr Redirection**: Redirects the postmaster's stderr and stdout to the logging pipe for collection by the logger process
6. **Resource Cleanup**: Closes file handles in the postmaster after successful launch

The function handles platform differences between Unix/Linux and Windows, using different APIs for pipe creation and file descriptor management.

## Parameters / Member Variables
- No parameters (void function)
- Returns: Process ID of the launched syslogger process, or 0 on failure

## Dependencies
- Functions called/Symbols referenced:
  - pipe/CreatePipe (creates the logging pipe)
  - MakePGDirectory (ensures log directory exists)
  - [logfile_getname](../l/logfile_getname.md) (generates log file names)
  - [logfile_open](../l/logfile_open.md) (opens log files)
  - [syslogger_fdget](../s/syslogger_fdget.md) (gets file descriptors for EXEC_BACKEND)
  - [postmaster_child_launch](../p/postmaster_child_launch.md) (launches the logger process)
  - dup2 (redirects stderr/stdout to the pipe)
- Called from (representative examples):
  - [PostmasterMain](../P/PostmasterMain.md) (initial startup)
  - [ServerLoop](ServerLoop.md) (during normal operation)
  - [process_pm_child_exit](../p/process_pm_child_exit.md) (when restarting failed logger)

## Notes and Other Information
- Only runs when Logging_collector is enabled in configuration
- Creates persistent pipes that survive logger process restarts
- Handles both regular stderr logging and structured logging (CSV, JSON formats)
- Implements robust error handling with FATAL errors for critical failures
- Uses different code paths for EXEC_BACKEND builds (Windows) vs fork-based systems
- The function ensures that log files are writable before launching the logger process to catch permission issues early
- After successful launch, the postmaster no longer writes directly to log files, instead sending all output through the pipe to the logger process