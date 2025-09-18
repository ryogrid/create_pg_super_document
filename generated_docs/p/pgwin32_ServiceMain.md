# pgwin32_ServiceMain

## Location
src/bin/pg_ctl/pg_ctl.c: 1613 - 1705

## Overview
The main entry point for PostgreSQL when running as a Windows service, handling the complete service lifecycle from startup through shutdown.

## Definition
```c
static void WINAPI pgwin32_ServiceMain(DWORD argc, LPTSTR *argv)
```

## Detailed Description
This function implements the complete lifecycle management for PostgreSQL running as a Windows service. It serves as the service's main thread and is responsible for initializing the service status structure, registering the control handler, starting the PostgreSQL postmaster process, monitoring its execution, and handling graceful shutdown.

The function begins by initializing the Windows service status structure with appropriate values, including accepted control commands (stop, shutdown, pause/continue). It then registers the service control handler, creates necessary synchronization objects, and launches the postmaster process using CreateRestrictedProcess for security.

During operation, it monitors both shutdown events and the postmaster process state using WaitForMultipleObjects. When shutdown is requested, it sends a SIGINT signal to the postmaster and monitors the shutdown process with periodic checkpoints to the Service Control Manager. The function ensures proper cleanup of handles and resources before terminating.

## Parameters / Member Variables
- `argc`: Number of arguments passed to the service (typically unused)
- `argv`: Array of string arguments passed to the service (typically unused)

## Dependencies
- Functions called/Symbols referenced:
  - [read_post_opts](../r/read_post_opts.md)
  - RegisterServiceCtrlHandler (Windows API)
  - [pgwin32_ServiceHandler](pgwin32_ServiceHandler.md)
  - CreateEvent (Windows API)
  - [pgwin32_SetServiceStatus](pgwin32_SetServiceStatus.md)
  - [pgwin32_CommandLine](pgwin32_CommandLine.md)
  - [CreateRestrictedProcess](../C/CreateRestrictedProcess.md)
  - CloseHandle (Windows API)
  - [write_eventlog](../w/write_eventlog.md)
  - [wait_for_postmaster_start](../w/wait_for_postmaster_start.md)
  - WaitForMultipleObjects (Windows API)
  - kill (signal function)
  - WaitForSingleObject (Windows API)
  - SetServiceStatus (Windows API)
- Uses global variables:
  - `status` (SERVICE_STATUS structure)
  - `hStatus` (service status handle)
  - `shutdownEvent` (Windows event object)
  - `postmasterPID` (process ID)
  - `postmasterProcess` (process handle)
  - `do_wait` (configuration flag)
  - `shutdownHandles` (array of handles to wait on)
- Called from (representative examples):
  - [pgwin32_doRunAsService](pgwin32_doRunAsService.md)

## Notes and Other Information
- This is a Windows-specific function used only in pg_ctl on Windows platforms
- Uses WINAPI calling convention required for Windows service main functions
- Implements comprehensive error handling and logging through Windows Event Log
- Provides progress reporting to SCM through checkpoint updates during shutdown
- Handles both graceful shutdown (via SIGINT) and process termination detection
- Includes timeout handling (12 checkpoints × 5 seconds = 60 seconds) for shutdown operations
- Ensures proper resource cleanup even in error conditions
- The service accepts STOP, SHUTDOWN, and PAUSE_CONTINUE control commands