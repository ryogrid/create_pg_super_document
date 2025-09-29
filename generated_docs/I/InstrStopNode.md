# InstrStopNode

## Location
[src/backend/executor/instrument.c:84-131](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/instrument.c#L84-L131)

## Overview
InstrStopNode completes performance instrumentation when exiting a plan node execution, calculating elapsed time, resource consumption, and tracking tuple counts and timing milestones.

## Definition
void InstrStopNode(Instrumentation *instr, double nTuples)

## Detailed Description
InstrStopNode is called at the completion of plan node execution to finalize performance measurements and accumulate statistics. The function performs several key operations: updates the total tuple count, calculates elapsed execution time by computing the difference between current time and the stored start time, accumulates buffer and WAL usage deltas using the previously established baselines, and tracks important timing milestones such as the time to first tuple. The function includes special handling for async mode execution where tuple emission patterns may differ from synchronous execution. It also includes safety checks to ensure proper pairing with InstrStartNode calls and resets timing state for potential subsequent executions.

## Parameters / Member Variables
- `instr`: Pointer to the Instrumentation structure containing measurement state and results
- `nTuples`: Number of tuples processed/returned during this execution cycle

## Dependencies
- Functions called/Symbols referenced:
  - INSTR_TIME_IS_ZERO (timing check macro)
  - INSTR_TIME_SET_CURRENT (timing capture macro)
  - INSTR_TIME_ACCUM_DIFF (timing accumulation macro)
  - INSTR_TIME_SET_ZERO (timing reset macro)
  - INSTR_TIME_GET_DOUBLE (timing conversion macro)
  - [BufferUsageAccumDiff](../B/BufferUsageAccumDiff.md) (buffer usage accumulation)
  - [WalUsageAccumDiff](../W/WalUsageAccumDiff.md) (WAL usage accumulation)
  - pgBufferUsage (global buffer usage counter)
  - pgWalUsage (global WAL usage counter)
  - elog (error logging)
- Called from (representative examples):
  - [ExecCallTriggerFunc](../E/ExecCallTriggerFunc.md)
  - [AfterTriggerExecute](../A/AfterTriggerExecute.md)
  - [ExecAsyncRequest](../E/ExecAsyncRequest.md)
  - [standard_ExecutorRun](../s/standard_ExecutorRun.md)
  - [ExecProcNodeInstr](../E/ExecProcNodeInstr.md)
  - [MultiExecBitmapAnd](../M/MultiExecBitmapAnd.md)
  - [MultiExecBitmapIndexScan](../M/MultiExecBitmapIndexScan.md)
  - [MultiExecHash](../M/MultiExecHash.md)

## Notes and Other Information
The function tracks the 'firsttuple' timing metric which is crucial for query optimization as it indicates how quickly a node starts producing results. In async mode, special logic handles cases where tuple emission may be delayed or irregular. The function safely resets the start time after use, allowing for proper detection of instrumentation errors in subsequent calls. Buffer and WAL usage calculations rely on the previously processed BufferUsageAccumDiff and WalUsageAccumDiff functions to provide accurate delta measurements.

## Simplified Source

```c
void
InstrStopNode(Instrumentation *instr, double nTuples)
{
    double save_tuplecount = instr->tuplecount;
    instr->tuplecount += nTuples;

    // Calculate elapsed time if timing enabled
    if (instr->need_timer) {
        if (INSTR_TIME_IS_ZERO(instr->starttime))
            elog(ERROR, "InstrStopNode called without start");

        instr_time endtime;
        INSTR_TIME_SET_CURRENT(endtime);
        INSTR_TIME_ACCUM_DIFF(instr->counter, endtime, instr->starttime);
        INSTR_TIME_SET_ZERO(instr->starttime);
    }

    // Accumulate buffer and WAL usage deltas
    if (instr->need_bufusage)
        BufferUsageAccumDiff(&instr->bufusage, &pgBufferUsage, &instr->bufusage_start);

    if (instr->need_walusage)
        WalUsageAccumDiff(&instr->walusage, &pgWalUsage, &instr->walusage_start);

    // Track first tuple timing
    if (!instr->running) {
        instr->running = true;
        instr->firsttuple = INSTR_TIME_GET_DOUBLE(instr->counter);
    } else if (instr->async_mode && save_tuplecount < 1.0) {
        instr->firsttuple = INSTR_TIME_GET_DOUBLE(instr->counter);
    }
}
```