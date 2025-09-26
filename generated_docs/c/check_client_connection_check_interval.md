# check_client_connection_check_interval

## Location
[src/backend/tcop/postgres.c:3633-3653](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tcop/postgres.c#L3633-L3653)

## Overview
A GUC check hook function that validates the client_connection_check_interval configuration parameter, ensuring it's set to 0 on platforms that don't support connection state reporting.

## Definition

```c
bool
check_client_connection_check_interval(int *newval, void **extra, GucSource source)
```
## Detailed Description
This function serves as a validation hook for the PostgreSQL configuration parameter client_connection_check_interval. It performs platform-specific validation by checking if the system supports reporting closed connections through the WaitEventSetCanReportClosed() function. On platforms where this capability is not available, the function enforces that the parameter must be set to 0, preventing the use of client connection checking when the underlying system cannot detect connection state changes.

If validation fails on an unsupported platform, the function provides an error message indicating that the parameter must be set to 0.

## Parameters / Member Variables
- : Pointer to the new value being set for client_connection_check_interval
- : Pointer to extra data (unused in this function)
- : The source of the configuration change (GucSource enumeration)

## Dependencies
- Functions called/Symbols referenced:
  - [WaitEventSetCanReportClosed](../W/WaitEventSetCanReportClosed.md)
  - GUC_check_errdetail
  - GucSource
- Called from (representative examples):
  - Referenced in GUC_HOOKS_H (src/include/utils/guc_hooks.h:44)

## Notes and Other Information
- This function is part of PostgreSQL's GUC (Grand Unified Configuration) system
- Platform-dependent validation ensures client connection checking is only enabled where supported
- Returns true if validation passes, false if the configuration is invalid for the current platform
- The function prevents configuration of connection checking on platforms without proper support
- Related to PostgreSQL's ability to detect disconnected clients during long-running operations