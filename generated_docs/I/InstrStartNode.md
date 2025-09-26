# InstrStartNode

## Location
src/backend/executor/instrument.c: 68 - 83

## Overview
InstrStartNode captures the starting state for performance instrumentation when entering a plan node execution, recording timing, buffer usage, and WAL usage baselines.

## Definition
void InstrStartNode(Instrumentation *instr)

## Detailed Description
InstrStartNode is called at the beginning of plan node execution to establish baseline measurements for performance monitoring. The function captures three types of starting states: execution timing (if timer instrumentation is enabled), buffer usage statistics (if buffer instrumentation is enabled), and Write-Ahead Log usage statistics (if WAL instrumentation is enabled). The function includes a safety check to detect incorrect usage patterns where InstrStartNode is called twice without an intervening InstrStopNode call, which would corrupt timing measurements. The captured baseline values are later used by InstrStopNode to compute elapsed time and resource consumption during node execution.

## Parameters / Member Variables
- `instr`: Pointer to the Instrumentation structure that will record the starting state measurements

## Dependencies
- Functions called/Symbols referenced:
  - INSTR_TIME_SET_CURRENT_LAZY (timing macro)
  - pgBufferUsage (global buffer usage counter)
  - pgWalUsage (global WAL usage counter)
  - elog (error logging)
- Called from (representative examples):
  - ExecCallTriggerFunc
  - AfterTriggerExecute
  - ExecAsyncRequest
  - standard_ExecutorRun
  - ExecProcNodeInstr
  - MultiExecBitmapAnd
  - MultiExecBitmapIndexScan
  - MultiExecHash

## Notes and Other Information
The function uses lazy time setting (INSTR_TIME_SET_CURRENT_LAZY) which only captures time if timing instrumentation is actually needed, providing performance optimization. The safety check for double-entry helps detect instrumentation bugs during development. Buffer and WAL usage snapshots are taken directly from global counters, allowing for accurate delta calculations when the node execution completes.