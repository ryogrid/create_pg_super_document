# pgwin32_ServiceMain

## Location
[src/bin/pg_ctl/pg_ctl.c:1613-1705](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_ctl/pg_ctl.c#L1613-L1705)

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

## Simplified Source

```c
static void WINAPI
pgwin32_ServiceMain(DWORD argc, LPTSTR *argv)
{
    PROCESS_INFORMATION pi;
    DWORD ret;

    // Initialize service status structure
    status.dwWin32ExitCode = S_OK;
    status.dwCheckPoint = 0;
    status.dwWaitHint = 60000;
    status.dwServiceType = SERVICE_WIN32_OWN_PROCESS;
    status.dwControlsAccepted = SERVICE_ACCEPT_STOP | SERVICE_ACCEPT_SHUTDOWN | SERVICE_ACCEPT_PAUSE_CONTINUE;
    status.dwServiceSpecificExitCode = 0;
    status.dwCurrentState = SERVICE_START_PENDING;

    memset(&pi, 0, sizeof(pi));
    read_post_opts();

    // Register service control handler
    if ((hStatus = RegisterServiceCtrlHandler(register_servicename, pgwin32_ServiceHandler)) == 0)
        return;

    // Create shutdown event
    if ((shutdownEvent = CreateEvent(NULL, true, false, NULL)) == NULL)
        return;

    // Start PostgreSQL postmaster process
    pgwin32_SetServiceStatus(SERVICE_START_PENDING);
    if (!CreateRestrictedProcess(pgwin32_CommandLine(false), &pi, true)) {
        pgwin32_SetServiceStatus(SERVICE_STOPPED);
        return;
    }

    postmasterPID = pi.dwProcessId;
    postmasterProcess = pi.hProcess;
    CloseHandle(pi.hThread);

    // Wait for startup if configured
    if (do_wait) {
        write_eventlog(EVENTLOG_INFORMATION_TYPE, "Waiting for server startup...");
        if (wait_for_postmaster_start(postmasterPID, true) != POSTMASTER_READY) {
            write_eventlog(EVENTLOG_ERROR_TYPE, "Timed out waiting for server startup");
            pgwin32_SetServiceStatus(SERVICE_STOPPED);
            return;
        }
        write_eventlog(EVENTLOG_INFORMATION_TYPE, "Server started and accepting connections");
    }

    // Service is now running
    pgwin32_SetServiceStatus(SERVICE_RUNNING);

    // Wait for shutdown event or process termination
    ret = WaitForMultipleObjects(2, shutdownHandles, FALSE, INFINITE);

    pgwin32_SetServiceStatus(SERVICE_STOP_PENDING);

    if (ret == WAIT_OBJECT_0) {  // shutdown event
        int maxShutdownCheckPoint = status.dwCheckPoint + 12;

        // Send shutdown signal and wait with periodic checkpoints
        kill(postmasterPID, SIGINT);
        while (WaitForSingleObject(postmasterProcess, 5000) == WAIT_TIMEOUT &&
               status.dwCheckPoint < maxShutdownCheckPoint) {
            status.dwCheckPoint++;
            SetServiceStatus(hStatus, (LPSERVICE_STATUS) &status);
        }
    }
    // else: postmaster went down on its own

    // Cleanup and stop service
    CloseHandle(shutdownEvent);
    CloseHandle(postmasterProcess);
    pgwin32_SetServiceStatus(SERVICE_STOPPED);
}
```