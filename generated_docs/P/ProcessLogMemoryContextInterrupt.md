# ProcessLogMemoryContextInterrupt

## Location
src/backend/utils/mmgr/mcxt.c: 1288 - 1315

## Overview
ProcessLogMemoryContextInterrupt performs the actual logging of memory contexts for the current backend process, implementing the deferred work from HandleLogMemoryContextInterrupt in a safe context outside of signal handlers.

## Definition
```c
void ProcessLogMemoryContextInterrupt(void)
```

## Detailed Description
This function is responsible for actually performing memory context logging after a signal has been received. It clears the LogMemoryContextPending flag, logs a message indicating the start of memory context logging for the current process, and then generates detailed memory context statistics. To prevent excessive disk usage when dealing with processes consuming large amounts of memory, it limits both the depth of the context hierarchy and the number of child contexts logged per parent to 100.

## Parameters / Member Variables
- None (void function with no parameters)

## Dependencies
- Functions called/Symbols referenced:
  - ereport (with LOG_SERVER_ONLY)
  - errhidestmt
  - errhidecontext
  - MemoryContextStatsDetail
  - MyProcPid (global variable)
  - TopMemoryContext (global variable)
- Called from (representative examples):
  - ProcessInterrupts
  - HandleMainLoopInterrupts
  - HandleAutoVacLauncherInterrupts
  - HandleCheckpointerInterrupts

## Notes and Other Information
- Called from CHECK_FOR_INTERRUPTS() macro in backend processes
- Uses LOG_SERVER_ONLY to prevent the message from being sent to connected clients
- Implements protective limits (100 max depth, 100 max children) to prevent disk space issues
- Part of PostgreSQL's debugging and monitoring infrastructure for memory usage analysis
- The logging output helps diagnose memory consumption patterns in backend processes