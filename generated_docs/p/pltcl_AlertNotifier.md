# pltcl_AlertNotifier

## Location
[src/pl/tcl/pltcl.c:368-372](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/pl/tcl/pltcl.c#L368-L372)

## Overview
A stub implementation of Tcl's notifier alert function that intentionally does nothing to prevent multithreading issues in PostgreSQL's PL/Tcl environment.

## Definition

```c
static void
pltcl_AlertNotifier(ClientData clientData)
```
## Detailed Description
This function is part of PostgreSQL's PL/Tcl implementation strategy to override Tcl's builtin Notifier subsystem. The function is intentionally empty as a safety measure to prevent the PostgreSQL backend from becoming multithreaded, which would break PostgreSQL's single-threaded architecture.

The Tcl notifier subsystem normally handles event processing and can introduce multithreading when the Tcl library is compiled with thread support (TCL_THREADS). By providing empty stub implementations of all notifier functions, PostgreSQL ensures that while the notifier capabilities are initialized, they never actually perform any operations that could cause threading issues.

This approach is safe because PostgreSQL never enters the Tcl event loop, so the notifier capabilities are initialized but never actively used in a way that would require actual implementation.

## Parameters / Member Variables
- `clientData`: Opaque pointer to client-specific data, passed through from Tcl's notifier system but unused in this stub implementation
## Dependencies
- Functions called/Symbols referenced:
  - (None - this is an empty stub function)
- Called from (representative examples):
  - [_PG_init](../P/_PG_init.md) (assigned to notifier.alertNotifierProc)

## Notes and Other Information
- This function is part of a complete set of Tcl notifier overrides that includes pltcl_InitNotifier, pltcl_FinalizeNotifier, pltcl_SetTimer, pltcl_CreateFileHandler, pltcl_DeleteFileHandler, pltcl_ServiceModeHook, and pltcl_WaitForEvent
- The empty implementation is intentional and by design - it prevents potential threading issues while maintaining compatibility with Tcl's notifier interface
- Located in src/pl/tcl/pltcl.c:368-372