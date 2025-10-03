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

## Simplified Source

```c
// Simplified version of pgwin32_is_service
int pgwin32_is_service(void) {
    static int cached_result = -1;
    BOOL is_member;
    PSID service_sid, local_system_sid;
    SID_IDENTIFIER_AUTHORITY nt_authority = {SECURITY_NT_AUTHORITY};
    HANDLE stderr_handle;

    // Return cached result if already determined
    if (cached_result != -1)
        return cached_result;

    // Check 1: Services typically don't have valid stderr
    stderr_handle = GetStdHandle(STD_ERROR_HANDLE);
    if (stderr_handle != INVALID_HANDLE_VALUE && stderr_handle != NULL) {
        cached_result = 0;  // Not a service
        return cached_result;
    }

    // Check 2: Are we running as LocalSystem?
    if (AllocateAndInitializeSid(&nt_authority, 1, SECURITY_LOCAL_SYSTEM_RID,
                                 0, 0, 0, 0, 0, 0, 0, &local_system_sid)) {
        if (CheckTokenMembership(NULL, local_system_sid, &is_member)) {
            FreeSid(local_system_sid);
            if (is_member) {
                cached_result = 1;  // Is a service
                return cached_result;
            }
        } else {
            FreeSid(local_system_sid);
            return -1;  // Error checking membership
        }
    } else {
        return -1;  // Error getting LocalSystem SID
    }

    // Check 3: Do we have service group membership?
    if (AllocateAndInitializeSid(&nt_authority, 1, SECURITY_SERVICE_RID,
                                 0, 0, 0, 0, 0, 0, 0, &service_sid)) {
        if (CheckTokenMembership(NULL, service_sid, &is_member)) {
            FreeSid(service_sid);
            cached_result = is_member ? 1 : 0;
        } else {
            FreeSid(service_sid);
            return -1;  // Error checking membership
        }
    } else {
        return -1;  // Error getting Service SID
    }

    return cached_result;
}
```

Key simplifications made:
- Removed detailed error messages for clarity (kept error return codes)
- Consolidated similar SID allocation and membership checking patterns
- Used more descriptive variable names (cached_result vs _is_service)
- Simplified the final result assignment logic
- Focused on the three main detection criteria without low-level error handling details
- Maintained the essential caching mechanism and all three service detection methods