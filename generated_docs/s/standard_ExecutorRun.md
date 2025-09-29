# standard_ExecutorRun

## Location
[src/backend/executor/execMain.c:310-399](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execMain.c#L310-L399)

## Overview
standard_ExecutorRun is the default implementation that performs the actual query plan execution, coordinating tuple retrieval and destination receiver management while tracking execution statistics.

## Definition
```c
void standard_ExecutorRun(QueryDesc *queryDesc, ScanDirection direction, uint64 count, bool execute_once)
```

## Detailed Description
standard_ExecutorRun orchestrates the core execution of query plans in PostgreSQL. This function manages the complete execution lifecycle including memory context switching, instrumentation setup, destination receiver coordination, and the actual plan execution through ExecutePlan.

The function determines whether tuples need to be sent based on the operation type (SELECT queries or statements with RETURNING clauses) and manages the destination receiver startup and shutdown accordingly. It handles execution statistics by tracking both per-call processed tuples (es_processed) and cumulative totals across multiple ExecutorRun calls (es_total_processed).

Key execution flow includes:
- Memory context management for proper resource isolation
- Optional instrumentation for performance monitoring  
- Destination receiver lifecycle management for tuple output
- Conditional plan execution based on scan direction
- Comprehensive execution statistics tracking

The function supports various scan directions and respects tuple count limits while ensuring proper cleanup and resource management.

## Parameters / Member Variables
- `queryDesc`: A QueryDesc structure containing the initialized query execution context with valid estate and snapshot
- `direction`: ScanDirection controlling scan behavior; NoMovementScanDirection skips plan execution
- `count`: Maximum number of tuples to retrieve; 0 means unlimited execution
- `execute_once`: Legacy parameter for API compatibility, not actively used in current implementation

## Dependencies
- Functions called/Symbols referenced:
  - [GetActiveSnapshot](../G/GetActiveSnapshot.md) (validates snapshot consistency)
  - [InstrStartNode](../I/InstrStartNode.md)/InstrStopNode (execution instrumentation)
  - ScanDirectionIsNoMovement (direction validation)
  - [ExecutePlan](../E/ExecutePlan.md) (core plan execution engine)
  - CmdType (command type enumeration)
  - [DestReceiver](../D/DestReceiver.md) (destination receiver interface)
  - CMD_SELECT (SELECT command type constant)
- Called from (representative examples):
  - [ExecutorRun](../E/ExecutorRun.md) (src/backend/executor/execMain.c:306)

## Notes and Other Information
- Requires ExecutorStart to have been called previously to establish proper execution state
- Memory context switching ensures execution occurs in the per-query context for proper resource management
- Destination receiver is only activated for operations that produce output tuples (SELECT or RETURNING clauses)
- Execution statistics are maintained at both per-call (es_processed) and cumulative (es_total_processed) levels
- [Instrumentation](../I/Instrumentation.md) support allows for detailed performance monitoring when enabled
- The function handles both tuple-producing and non-tuple-producing operations appropriately
- Proper cleanup ensures destination receivers are shut down even if execution fails
- Located at src/backend/executor/execMain.c:310-399
- The execute_once parameter is retained for API compatibility but does not affect execution behavior

## Simplified Source

```c
void
standard_ExecutorRun(QueryDesc *queryDesc,
                    ScanDirection direction, uint64 count, bool execute_once)
{
    EState *estate;
    CmdType operation;
    DestReceiver *dest;
    bool sendTuples;
    MemoryContext oldcontext;

    // Validate input parameters
    Assert(queryDesc != NULL);
    estate = queryDesc->estate;
    Assert(estate != NULL);
    Assert(!(estate->es_top_eflags & EXEC_FLAG_EXPLAIN_ONLY));
    Assert(GetActiveSnapshot() == estate->es_snapshot);

    // Switch to per-query memory context
    oldcontext = MemoryContextSwitchTo(estate->es_query_cxt);

    // Start execution timing if instrumentation is enabled
    if (queryDesc->totaltime)
        InstrStartNode(queryDesc->totaltime);

    // Extract execution information
    operation = queryDesc->operation;
    dest = queryDesc->dest;

    // Initialize tuple processing counter
    estate->es_processed = 0;

    // Determine if this operation sends tuples to output
    sendTuples = (operation == CMD_SELECT ||
                  queryDesc->plannedstmt->hasReturning);

    // Initialize destination receiver if needed
    if (sendTuples)
        dest->rStartup(dest, operation, queryDesc->tupDesc);

    // Execute the plan if scan direction allows movement
    if (!ScanDirectionIsNoMovement(direction))
        ExecutePlan(queryDesc, operation, sendTuples, count, direction, dest);

    // Update cumulative tuple count across ExecutorRun calls
    estate->es_total_processed += estate->es_processed;

    // Clean up destination receiver
    if (sendTuples)
        dest->rShutdown(dest);

    // Stop execution timing
    if (queryDesc->totaltime)
        InstrStopNode(queryDesc->totaltime, estate->es_processed);

    // Restore original memory context
    MemoryContextSwitchTo(oldcontext);
}
```