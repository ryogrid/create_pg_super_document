# pgwin32_ServiceHandler

## Location
[src/bin/pg_ctl/pg_ctl.c:1581-1612](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_ctl/pg_ctl.c#L1581-L1612)

## Overview
Handles Windows service control requests from the Service Control Manager, processing commands like stop, shutdown, and configuration reload for the PostgreSQL service.

## Definition
```c
static void WINAPI pgwin32_ServiceHandler(DWORD request)
```

## Detailed Description
This function serves as the callback handler for Windows service control requests. It is registered with the Windows Service Control Manager and is invoked when the system or administrators send control commands to the PostgreSQL service. The function processes different types of service control requests including shutdown, stop, pause (used for configuration reload), continue, and interrogate commands.

For stop and shutdown requests, it sets a wait hint of 10 seconds, updates the service status to SERVICE_STOP_PENDING, and signals the shutdown event to initiate graceful service termination. For pause requests, it sends a SIGHUP signal to the postmaster process to trigger configuration reload, which is the PostgreSQL equivalent of pausing/reloading.

## Parameters / Member Variables
- `request`: A DWORD value representing the service control request type (SERVICE_CONTROL_STOP, SERVICE_CONTROL_SHUTDOWN, SERVICE_CONTROL_PAUSE, etc.)

## Dependencies
- Functions called/Symbols referenced:
  - [pgwin32_SetServiceStatus](pgwin32_SetServiceStatus.md)
  - SetEvent (Windows API)
  - kill (signal function)
- Uses global variables:
  - `status` (SERVICE_STATUS structure)
  - `shutdownEvent` (Windows event object)
  - `postmasterPID` (process ID of PostgreSQL main process)
  - `SIGHUP` (signal constant)
- Called from (representative examples):
  - Windows Service Control Manager (via pgwin32_ServiceMain registration)

## Notes and Other Information
- This is a Windows-specific function used only in pg_ctl on Windows platforms
- Uses WINAPI calling convention required for Windows service callbacks
- The SERVICE_CONTROL_PAUSE is repurposed for PostgreSQL configuration reload via SIGHUP
- Wait hints are provided to inform the SCM about expected operation duration
- Graceful shutdown is implemented through event signaling rather than direct termination
- The function includes FIXME comment suggesting future expansion for additional signal handling