# pgwin32_doUnregister

## Location
src/bin/pg_ctl/pg_ctl.c: 1535 - 1573

## Overview
Unregisters PostgreSQL from the Windows Service Control Manager (SCM), removing the service entry and preventing it from running as a Windows service.

## Definition
```c
static void pgwin32_doUnregister(void)
```

## Detailed Description
This function handles the removal of PostgreSQL service registration from the Windows Service Control Manager. It opens a connection to the SCM, verifies that the service is currently registered, opens the specific service for deletion, and then removes it from the system. The function includes comprehensive error handling at each step, providing detailed error messages and exit codes when operations fail.

The unregistration process involves opening the service with DELETE permissions and calling the Windows DeleteService API to permanently remove the service from the SCM database.

## Parameters / Member Variables
This function takes no parameters but relies on global variables:
- Uses `register_servicename` to identify the service to unregister

## Dependencies
- Functions called/Symbols referenced:
  - OpenSCManager (Windows API)
  - [write_stderr](../w/write_stderr.md)
  - [pgwin32_IsInstalled](pgwin32_IsInstalled.md)
  - OpenService (Windows API)
  - DeleteService (Windows API)
  - CloseServiceHandle (Windows API)
  - GetLastError (Windows API)
- Called from (representative examples):
  - [main](../m/main.md)

## Notes and Other Information
- This is a Windows-specific function used only in pg_ctl on Windows platforms
- Requires appropriate privileges to unregister services in Windows
- The function verifies service existence before attempting deletion to provide meaningful error messages
- Proper cleanup of service handles is performed even on error conditions
- Error handling includes specific error codes from Windows API calls for debugging purposes