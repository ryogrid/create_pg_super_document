# pgwin32_is_service

## Location
[src/port/win32security.c:120-190](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/port/win32security.c#L120-L190)

## Overview
A Windows-specific function that determines whether PostgreSQL is running as a Windows service by checking multiple service-related indicators and caching the result for subsequent calls.

## Definition

```c
int
pgwin32_is_service(void)
```
## Detailed Description
The  function implements a comprehensive detection mechanism to determine if PostgreSQL is running as a Windows service. It employs a multi-criteria approach using three distinct checks:

1. **Standard Error Handle Validation**: Services typically don't have valid standard error handles, so the absence of a valid stderr handle suggests service execution.

2. **LocalSystem Account Detection**: Checks if the process is running under the LocalSystem account, which is commonly used for Windows services and has special privileges.

3. **Service Group Membership**: Verifies if the process token contains , which is automatically added by the Windows Service Control Manager (SCM) when starting a service.

The function uses static caching () to ensure the determination is made only once per process, improving performance for subsequent calls. This is important because service status doesn't change during process lifetime.

Error handling is limited to direct stderr writes using  rather than PostgreSQL's standard error reporting mechanisms, as this function is called early in the startup process before those systems are available.

## Parameters / Member Variables
- None (void function)

## Dependencies
- Functions called/Symbols referenced:
  - GetStdHandle (Windows API)
  - AllocateAndInitializeSid (Windows API)
  - CheckTokenMembership (Windows API)
  - FreeSid (Windows API)
  - GetLastError (Windows API)
  - fprintf (standard C library)
- Called from (representative examples):
  - [send_message_to_server_log](../s/send_message_to_server_log.md) (in src/backend/utils/error/elog.c)
  - [write_stderr](../w/write_stderr.md) (in src/backend/utils/error/elog.c)
  - Referenced in src/include/port/win32_port.h

## Notes and Other Information
- Returns 0 for non-service processes, 1 for service processes, and -1 for errors
- Uses static caching to avoid repeated expensive system calls - the result is determined only on first call
- Cannot use  or  for error reporting due to being called early in startup
- The LocalSystem check is necessary because services running as LocalSystem surprisingly don't have  in their token
- Critical for PostgreSQL's logging and error handling behavior on Windows, as services have different I/O characteristics
- The function is Windows-specific and located in the port-specific directory structure
- Used by error logging systems to adapt behavior based on execution context (service vs. interactive)