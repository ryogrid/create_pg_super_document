# InstrEndLoop

## Location
src/backend/executor/instrument.c: 140 - 168

## Overview
Finalizes instrumentation data collection for a plan node execution cycle by accumulating per-cycle statistics into totals and resetting for the next cycle.

## Definition
void InstrEndLoop(Instrumentation *instr)

## Detailed Description
InstrEndLoop is a critical function in PostgreSQLs query execution instrumentation system that concludes a single execution cycle for a plan node. It performs essential bookkeeping by accumulating timing and tuple count statistics from the current cycle into running totals, then resets the instrumentation state for potential subsequent cycles. The function includes safety checks to ensure its not called on nodes that are still running or have already been shut down.

## Parameters / Member Variables
- instr: Pointer to the Instrumentation structure containing timing and execution statistics for the plan node

## Dependencies
- Functions called/Symbols referenced:
  - INSTR_TIME_IS_ZERO (macro for checking zero time values)
  - INSTR_TIME_GET_DOUBLE (macro for converting time to double)
  - INSTR_TIME_SET_ZERO (macro for zeroing time values)
  - elog (error logging function)
- Called from (representative examples):
  - report_triggers (in explain.c for trigger reporting)
  - ExplainNode (in explain.c for query plan explanation)
  - show_modifytable_info (in explain.c for modify table information)
  - ExecReScan (in execAmi.c for plan node rescanning)
  - ExecParallelReportInstrumentation (in execParallel.c for parallel execution reporting)

## Notes and Other Information
- The function performs error checking to ensure its not called on a node that is still actively running
- Accumulates startup time, total execution time, tuple counts, and loop counts into running totals
- Resets per-cycle counters (starttime, counter, firsttuple, tuplecount) for potential reuse
- Sets the running flag to false to indicate the cycle is complete
- This function is part of PostgreSQLs EXPLAIN ANALYZE functionality that provides detailed execution statistics