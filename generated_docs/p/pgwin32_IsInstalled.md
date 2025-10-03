# pgwin32_IsInstalled

## Location
[src/bin/pg_ctl/pg_ctl.c:1406-1416](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_ctl/pg_ctl.c#L1406-L1416)

## Overview
Checks whether a PostgreSQL service is installed in the Windows Service Control Manager.

## Definition

```c
static bool
pgwin32_IsInstalled(SC_HANDLE hSCM)
```
## Detailed Description
This function determines if a PostgreSQL service is already registered with the Windows Service Control Manager (SCM). It performs a lightweight check by attempting to open the service using  with  access rights, which only requires permission to read the service configuration.

The function follows proper Windows API patterns:
1. **Service Query**: Attempts to open the service using the global  variable
2. **Result Evaluation**: Returns true if the service handle is valid (service exists)
3. **Resource Cleanup**: Properly closes the service handle if it was successfully opened
4. **Boolean Return**: Provides a simple true/false result for service existence

This is typically used before service registration or unregistration operations to verify the current state.

## Parameters / Member Variables
- `hSCM`: Handle to the Service Control Manager, obtained from
## Dependencies
- Functions called/Symbols referenced:
  - OpenService (Windows API)
  - CloseServiceHandle (Windows API)
  - register_servicename (global variable)
- Called from (representative examples):
  - [pgwin32_doRegister](pgwin32_doRegister.md) (src/bin/pg_ctl/pg_ctl.c:1511)
  - [pgwin32_doUnregister](pgwin32_doUnregister.md) (src/bin/pg_ctl/pg_ctl.c:1545)

## Notes and Other Information
- The function is static and Windows-specific, only used within pg_ctl.c
- Uses minimal access rights (SERVICE_QUERY_CONFIG) for the service check
- Properly manages Windows service handles to avoid resource leaks
- The service name to check is determined by the global  variable
- Returns immediately without detailed error analysis - focused on existence check only
- Part of pg_ctl's Windows service management infrastructure