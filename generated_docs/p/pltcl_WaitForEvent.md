# pltcl_WaitForEvent

## Location
[src/pl/tcl/pltcl.c:389-403](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/pl/tcl/pltcl.c#L389-L403)

## Overview
A stub implementation of Tcl's event waiting function that always returns 0 to prevent multithreading issues in PostgreSQL's PL/Tcl environment.

## Definition

```c
static int
pltcl_WaitForEvent(CONST86 Tcl_Time *timePtr)
```
## Detailed Description
This function is part of PostgreSQL's PL/Tcl implementation strategy to override Tcl's builtin Notifier subsystem. Unlike the other notifier functions that are completely empty, this function returns 0 to indicate that no events are available for processing.

In a normal Tcl environment, this function would wait for events (such as file I/O, timers, or other notifications) to become available, potentially blocking until an event occurs or a timeout expires. The function would return a positive value if events were processed, or 0 if the timeout was reached without processing any events.

By always returning 0, PostgreSQL's implementation ensures that the Tcl event loop (if it were ever entered) would immediately conclude that no events are available, preventing any blocking behavior that could interfere with PostgreSQL's single-threaded architecture.

## Parameters / Member Variables
- `*timePtr`: Pointer to a Tcl_Time structure specifying the maximum time to wait for events (unused in this stub implementation, but would normally define timeout behavior)
## Dependencies
- Functions called/Symbols referenced:
  - CONST86 (Tcl compatibility macro for const qualifier)
- Called from (representative examples):
  - [_PG_init](../P/_PG_init.md) (assigned to notifier.waitForEventProc)

## Notes and Other Information
- This function is part of a complete set of Tcl notifier overrides that includes pltcl_InitNotifier, pltcl_FinalizeNotifier, pltcl_SetTimer, pltcl_AlertNotifier, pltcl_CreateFileHandler, pltcl_DeleteFileHandler, and pltcl_ServiceModeHook
- Unlike the other stub functions which are completely empty, this function returns a value (0) to properly satisfy the Tcl notifier interface expectations
- The return value of 0 indicates that no events were processed, which is the desired behavior to prevent event loop execution
- CONST86 is a Tcl compatibility macro that expands to 'const' on modern systems but provides compatibility with older Tcl versions
- Located in src/pl/tcl/pltcl.c:389-392

## Simplified Source

```c
static int
pltcl_WaitForEvent(CONST86 Tcl_Time *timePtr)
{
    // Always return 0 to indicate no events are available
    // Prevents Tcl event loop from blocking PostgreSQL's execution
    // Normal Tcl would wait for file/timer events here
    return 0;
}
```