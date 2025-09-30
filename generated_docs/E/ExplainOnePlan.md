# ExplainOnePlan

## Location
[src/backend/commands/explain.c:617-806](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/explain.c#L617-L806)

## Overview
ExplainOnePlan executes a planned query (if analysis is requested) and generates comprehensive EXPLAIN output including execution statistics, buffer usage, memory counters, and timing information.

## Definition

```c
struct config_generic **gucs;
```
## Detailed Description
ExplainOnePlan is the core function responsible for executing planned queries and generating detailed EXPLAIN output. It handles both EXPLAIN (plan-only) and EXPLAIN ANALYZE (with execution) scenarios.

The function performs several key operations:
1. Sets up instrumentation options based on the requested explain options (timing, buffers, WAL)
2. Creates appropriate destination receivers for different scenarios (INTO clauses, serialization)
3. Executes the query if ANALYZE is requested, collecting runtime statistics
4. Generates comprehensive output including plan structure, execution statistics, buffer usage, memory usage, JIT information, trigger statistics, and serialization metrics
5. Manages snapshots and command counter increments for proper transaction handling

The function is exported for use by prepare.c in EXPLAIN EXECUTE scenarios and for potential index advisor plugins.

## Parameters / Member Variables
- : The planned statement to execute and explain
- : IntoClause for CREATE TABLE AS statements, NULL otherwise  
- : ExplainState containing output formatting options and state
- : Original query string for context and error messages
- : Parameter list for parameterized queries
- : Query environment providing additional context
- : Planning time duration for summary reporting
- : Buffer usage statistics from planning phase
- : Memory usage counters from planning phase

## Dependencies
- Functions called/Symbols referenced:
  - [CreateQueryDesc](../C/CreateQueryDesc.md)
  - [ExecutorStart](ExecutorStart.md)
  - [ExecutorRun](ExecutorRun.md)  
  - [ExecutorFinish](ExecutorFinish.md)
  - [ExecutorEnd](ExecutorEnd.md)
  - [ExplainPrintPlan](ExplainPrintPlan.md)
  - [ExplainPrintTriggers](ExplainPrintTriggers.md)
  - [ExplainPrintJITSummary](ExplainPrintJITSummary.md)
  - [ExplainPrintSerialize](ExplainPrintSerialize.md)
  - [CreateIntoRelDestReceiver](../C/CreateIntoRelDestReceiver.md)
  - [CreateExplainSerializeDestReceiver](../C/CreateExplainSerializeDestReceiver.md)
  - [PushCopiedSnapshot](../P/PushCopiedSnapshot.md)
  - [GetActiveSnapshot](../G/GetActiveSnapshot.md)
  - [UpdateActiveSnapshotCommandId](../U/UpdateActiveSnapshotCommandId.md)
  - [elapsed_time](../e/elapsed_time.md)
- Called from (representative examples):
  - [standard_ExplainOneQuery](../s/standard_ExplainOneQuery.md)
  - [ExplainExecuteQuery](ExplainExecuteQuery.md)

## Notes and Other Information
- The function always collects timing for the entire statement regardless of node-level timing settings
- For CREATE TABLE AS with ANALYZE, special handling ensures proper data flow to the destination table
- EXPLAIN ANALYZE CREATE TABLE AS WITH NO DATA uses NoMovementScanDirection to avoid data creation
- JIT information is tied to the costs option to avoid regression test output differences
- Serialization metrics are captured before destroying the destination receiver
- The function handles snapshot management to ensure proper visibility of previously executed statements
- Memory and buffer usage from both planning and execution phases are reported when available

## Simplified Source

```c
void
ExplainOnePlan(PlannedStmt *plannedstmt, IntoClause *into, ExplainState *es,
               const char *queryString, ParamListInfo params,
               QueryEnvironment *queryEnv, const instr_time *planduration,
               const BufferUsage *bufusage, const MemoryContextCounters *mem_counters)
{
    DestReceiver *dest;
    QueryDesc *queryDesc;
    instr_time starttime;
    double totaltime = 0;
    int eflags;
    int instrument_option = 0;
    SerializeMetrics serializeMetrics = {0};

    Assert(plannedstmt->commandType != CMD_UTILITY);

    // Set up instrumentation options based on explain settings
    if (es->analyze && es->timing)
        instrument_option |= INSTRUMENT_TIMER;
    else if (es->analyze)
        instrument_option |= INSTRUMENT_ROWS;

    if (es->buffers)
        instrument_option |= INSTRUMENT_BUFFERS;
    if (es->wal)
        instrument_option |= INSTRUMENT_WAL;

    INSTR_TIME_SET_CURRENT(starttime);

    // Set up snapshot for query execution
    PushCopiedSnapshot(GetActiveSnapshot());
    UpdateActiveSnapshotCommandId();

    // Create appropriate destination receiver
    if (into)
        dest = CreateIntoRelDestReceiver(into);
    else if (es->serialize != EXPLAIN_SERIALIZE_NONE)
        dest = CreateExplainSerializeDestReceiver(es);
    else
        dest = None_Receiver;

    // Create query descriptor
    queryDesc = CreateQueryDesc(plannedstmt, queryString,
                               GetActiveSnapshot(), InvalidSnapshot,
                               dest, params, queryEnv, instrument_option);

    // Set execution flags
    if (es->analyze)
        eflags = 0;  // Run to completion
    else
        eflags = EXEC_FLAG_EXPLAIN_ONLY;
    if (es->generic)
        eflags |= EXEC_FLAG_EXPLAIN_GENERIC;
    if (into)
        eflags |= GetIntoRelEFlags(into);

    // Start executor
    ExecutorStart(queryDesc, eflags);

    // Execute the plan if analysis is requested
    if (es->analyze) {
        ScanDirection dir = (into && into->skipData) ?
                           NoMovementScanDirection : ForwardScanDirection;

        ExecutorRun(queryDesc, dir, 0, true);
        ExecutorFinish(queryDesc);
        totaltime += elapsed_time(&starttime);
    }

    // Capture serialization metrics before destroying receiver
    if (es->serialize != EXPLAIN_SERIALIZE_NONE)
        serializeMetrics = GetSerializationMetrics(dest);

    dest->rDestroy(dest);

    // Generate explain output
    ExplainOpenGroup("Query", NULL, true, es);
    ExplainPrintPlan(es, queryDesc);

    // Show planning information
    if (peek_buffer_usage(es, bufusage) || mem_counters) {
        ExplainOpenGroup("Planning", "Planning", true, es);
        if (bufusage)
            show_buffer_usage(es, bufusage);
        if (mem_counters)
            show_memory_counters(es, mem_counters);
        ExplainCloseGroup("Planning", "Planning", true, es);
    }

    // Show planning time
    if (es->summary && planduration) {
        double plantime = INSTR_TIME_GET_DOUBLE(*planduration);
        ExplainPropertyFloat("Planning Time", "ms", 1000.0 * plantime, 3, es);
    }

    // Show execution information
    if (es->analyze)
        ExplainPrintTriggers(es, queryDesc);

    if (es->costs)
        ExplainPrintJITSummary(es, queryDesc);

    if (es->serialize != EXPLAIN_SERIALIZE_NONE)
        ExplainPrintSerialize(es, &serializeMetrics);

    // Final cleanup
    INSTR_TIME_SET_CURRENT(starttime);
    ExecutorEnd(queryDesc);
    FreeQueryDesc(queryDesc);
    PopActiveSnapshot();

    if (es->analyze)
        CommandCounterIncrement();

    totaltime += elapsed_time(&starttime);

    // Show execution time
    if (es->summary && es->analyze)
        ExplainPropertyFloat("Execution Time", "ms", 1000.0 * totaltime, 3, es);

    ExplainCloseGroup("Query", NULL, true, es);
}
```