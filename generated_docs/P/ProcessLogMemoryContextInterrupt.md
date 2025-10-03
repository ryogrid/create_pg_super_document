# ProcessLogMemoryContextInterrupt

## Location
[src/backend/utils/mmgr/mcxt.c:1288-1315](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mmgr/mcxt.c#L1288-L1315)

## Overview
ProcessLogMemoryContextInterrupt performs the actual logging of memory contexts for the current backend process, implementing the deferred work from HandleLogMemoryContextInterrupt in a safe context outside of signal handlers.

## Definition
```c
void ProcessLogMemoryContextInterrupt(void)
```

## Detailed Description
This function is responsible for actually performing memory context logging after a signal has been received. It clears the LogMemoryContextPending flag, logs a message indicating the start of memory context logging for the current process, and then generates detailed memory context statistics. To prevent excessive disk usage when dealing with processes consuming large amounts of memory, it limits both the depth of the context hierarchy and the number of child contexts logged per parent to 100.

## Parameters / Member Variables



## Dependencies
- Functions called/Symbols referenced:
  - ereport (with LOG_SERVER_ONLY)
  - [errhidestmt](../e/errhidestmt.md)
  - [errhidecontext](../e/errhidecontext.md)
  - [MemoryContextStatsDetail](../M/MemoryContextStatsDetail.md)
  - MyProcPid (global variable)
  - TopMemoryContext (global variable)
- Called from (representative examples):
  - [ProcessInterrupts](ProcessInterrupts.md)
  - [HandleMainLoopInterrupts](../H/HandleMainLoopInterrupts.md)
  - [HandleAutoVacLauncherInterrupts](../H/HandleAutoVacLauncherInterrupts.md)
  - [HandleCheckpointerInterrupts](../H/HandleCheckpointerInterrupts.md)

## Notes and Other Information
- Called from CHECK_FOR_INTERRUPTS() macro in backend processes
- Uses LOG_SERVER_ONLY to prevent the message from being sent to connected clients
- Implements protective limits (100 max depth, 100 max children) to prevent disk space issues
- Part of PostgreSQL's debugging and monitoring infrastructure for memory usage analysis
- The logging output helps diagnose memory consumption patterns in backend processes

## Simplified Source

```c
// Simplified version of ProcessLogMemoryContextInterrupt
void ProcessLogMemoryContextInterrupt(void) {
    // Step 1: Clear the pending interrupt flag
    LogMemoryContextPending = false;

    // Step 2: Log the start of memory context dump
    ereport(LOG_SERVER_ONLY,
            (errhidestmt(true),
             errhidecontext(true),
             errmsg("logging memory contexts of PID %d", MyProcPid)));

    // Step 3: Generate detailed memory context statistics with protective limits
    // Limit depth and children to 100 to prevent disk space issues
    MemoryContextStatsDetail(TopMemoryContext, 100, 100, false);
}
```

Key simplifications made:
- Removed detailed comments explaining the rationale (kept in documentation above)
- Consolidated the multi-line ereport call for better readability
- Added step-by-step comments explaining the three main actions
- Focused on the essential flow: clear flag → log message → dump stats