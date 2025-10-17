# pgwin32_doRegister

## Location
[src/bin/pg_ctl/pg_ctl.c:1501-1534](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_ctl/pg_ctl.c#L1501-L1534)

## Overview
Registers PostgreSQL as a Windows service in the Windows Service Control Manager (SCM), enabling it to run as a system service that can be started automatically at boot time.

## Definition

```c
static void
pgwin32_doRegister(void)
```
## Detailed Description
This function handles the registration of PostgreSQL as a Windows service. It opens a connection to the Windows Service Control Manager, checks if the service is already registered, and if not, creates a new service entry with the specified configuration parameters. The service is configured to run as a Win32 service with its own process, using the command line parameters appropriate for service execution.

The function performs error handling at each step, writing error messages to stderr and exiting with status code 1 if any operation fails. Upon successful registration, it cleans up the service handles.

## Parameters / Member Variables
This function takes no parameters but relies on global variables:
- Uses  for the service name
- Uses  and  for service credentials
- Uses  for service startup configuration

## Dependencies
- Functions called/Symbols referenced:
  - OpenSCManager (Windows API)
  - [pgwin32_IsInstalled](pgwin32_IsInstalled.md)
  - [write_stderr](../w/write_stderr.md)
  - CreateService (Windows API)
  - [pgwin32_CommandLine](pgwin32_CommandLine.md)
  - CloseServiceHandle (Windows API)
  - GetLastError (Windows API)
- Called from (representative examples):
  - [main](../m/main.md)

## Notes and Other Information
- This is a Windows-specific function used only in pg_ctl on Windows platforms
- Requires appropriate privileges to register services in Windows
- The service is configured with dependency on RPCSS (Remote Procedure Call System Service)
- Uses SERVICE_WIN32_OWN_PROCESS to run PostgreSQL in its own process space
- Error handling includes specific error codes from Windows API calls

## Simplified Source

```c
static void
pgwin32_doRegister(void)
{
    // Open Windows Service Control Manager
    SC_HANDLE hSCM = OpenSCManager(NULL, NULL, SC_MANAGER_ALL_ACCESS);
    if (!hSCM) {
        write_stderr("could not open service manager");
        exit(1);
    }

    // Check if service already exists
    if (pgwin32_IsInstalled(hSCM)) {
        CloseServiceHandle(hSCM);
        write_stderr("service already registered");
        exit(1);
    }

    // Create new Windows service entry
    SC_HANDLE hService = CreateService(hSCM, register_servicename, register_servicename,
                                       SERVICE_ALL_ACCESS, SERVICE_WIN32_OWN_PROCESS,
                                       pgctl_start_type, SERVICE_ERROR_NORMAL,
                                       pgwin32_CommandLine(true),
                                       NULL, NULL, "RPCSS\0",
                                       register_username, register_password);

    if (!hService) {
        CloseServiceHandle(hSCM);
        write_stderr("could not register service: error %lu", GetLastError());
        exit(1);
    }

    // Clean up handles
    CloseServiceHandle(hService);
    CloseServiceHandle(hSCM);
}
```