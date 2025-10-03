# pgwin32_doRunAsService

## Location
[src/bin/pg_ctl/pg_ctl.c:1706-1731](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_ctl/pg_ctl.c#L1706-L1731)

## Overview
Initializes the PostgreSQL server as a Windows service by registering the service main function with the Windows Service Control Manager.

## Definition

```c
static void
pgwin32_doRunAsService(void)
```
## Detailed Description
This function sets up the service table entry for PostgreSQL when running as a Windows service. It creates a SERVICE_TABLE_ENTRY structure that maps the registered service name to the main service function (pgwin32_ServiceMain), then calls StartServiceCtrlDispatcher to hand control over to the Windows Service Control Manager. If the service dispatcher fails to start, it outputs an error message and exits the program with status code 1.

## Parameters / Member Variables

## Dependencies
- Functions called/Symbols referenced:
  - StartServiceCtrlDispatcher (Windows API)
  - [pgwin32_ServiceMain](pgwin32_ServiceMain.md)
  - [write_stderr](../w/write_stderr.md)
  - GetLastError (Windows API)
  - exit
- Called from (representative examples):
  - [main](../m/main.md)

## Notes and Other Information
- This function is Windows-specific and only available when PostgreSQL is compiled for Windows platforms
- The function uses the global variable register_servicename to identify the service
- Error handling includes retrieving the Windows error code via GetLastError() for diagnostic purposes
- The function is marked as static, limiting its scope to the pg_ctl.c file
- This is part of PostgreSQL's Windows service integration functionality in the pg_ctl utility