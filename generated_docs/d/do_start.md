# do_start

## Location
src/bin/pg_ctl/pg_ctl.c: 923 - 1014

## Overview
Starts a PostgreSQL server process, handling configuration setup, process launching, and optional startup monitoring with proper signal handling and status reporting.

## Definition
```c
static void do_start(void)
```

## Detailed Description
This function implements the core server startup logic for pg_ctl, managing the complete lifecycle of PostgreSQL server launch. It handles multiple scenarios including fresh starts, restarts, and concurrent server detection.

The function performs these major operations:

1. **Pre-startup Checks**: Detects if another server might already be running (except during RESTART operations)
2. **Configuration Setup**: Reads startup options, locates executables, and configures environment
3. **Core File Management**: Optionally removes core file size limits for debugging
4. **Parent Process Tracking**: Sets environment variable for grandparent PID (non-Windows)  
5. **Server Launch**: Spawns the postmaster process through start_postmaster()
6. **Startup Monitoring**: If wait mode is enabled, monitors startup progress with timeout handling
7. **Signal Management**: Installs SIGINT handler to allow graceful startup interruption

The function provides different behaviors based on the wait flag - either returning immediately after launch or monitoring startup completion with detailed status reporting.

## Parameters / Member Variables
This function takes no parameters but operates on several global variables:
- `ctl_command`: Current pg_ctl command type (affects restart behavior)
- `pgdata_opt`: Data directory options (set to empty string for restarts)
- `exec_path`: PostgreSQL server executable path
- `do_wait`: Flag controlling whether to wait for startup completion
- `allow_core_files`: Flag for core file generation during crashes
- `postmasterPID`: Process ID of launched server (for signal handling)

## Dependencies
- Functions called/Symbols referenced:
  - `get_pgpid` (check for existing server process)
  - `read_post_opts` (load startup options)
  - `find_other_exec_or_die` (locate postgres executable)
  - `unlimit_core_size` (remove core file limits, if supported)
  - `start_postmaster` (launch server process)
  - `trap_sigint_during_startup` (SIGINT signal handler)
  - `pqsignal` (PostgreSQL signal handling)
  - `wait_for_postmaster_start` (monitor startup progress)
  - `print_msg` (status output)
  - `write_stderr` (error output)
  - Platform-specific functions: `getppid`, `setenv` (Unix), `CloseHandle` (Windows)

- Called from:
  - `main` (direct start command)
  - `do_restart` (during restart operations)

## Notes and Other Information
- The function warns but continues if another server appears to be running (except during restarts)
- Environment variable PG_GRANDPARENT_PID is set to help the postmaster identify the shell process tree
- Signal handling during startup allows users to interrupt with CTRL-C, which forwards the signal to the postmaster
- Three possible startup outcomes when waiting: READY (success), STILL_STARTING (timeout), FAILED (error)
- Windows-specific cleanup includes closing the postmaster process handle
- Core file limits are conditionally removed based on HAVE_GETRLIMIT compile-time flag
- The function uses different executable version strings for location verification (PG_BACKEND_VERSIONSTR vs PG_VERSION)
- Memory and resource cleanup is minimal since the function often leads to process termination