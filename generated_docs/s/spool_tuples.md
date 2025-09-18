# spool_tuples

## Location
src/backend/executor/nodeWindowAgg.c: 1241 - 1334

## Overview
This static function reads tuples from the outer node up to a specified position and stores them into the tuplestore, handling partition boundaries and different execution modes.

## Definition
```c
static void spool_tuples(WindowAggState *winstate, int64 pos)
```

## Detailed Description
The `spool_tuples` function is responsible for buffering input tuples from the outer plan into the WindowAgg tuplestore. It reads tuples up to a specified position (or the entire partition if pos is -1) while managing partition boundaries and different execution modes.

The function handles several execution scenarios: normal operation, pass-through modes (where tuples may not need to be stored), and performance optimizations when the tuplestore has spilled to disk. It detects partition boundaries by comparing tuples against partition key equality expressions and appropriately manages the transition between partitions.

The function operates in the query memory context when calling the outer plan and includes optimizations such as spooling entire partitions when the tuplestore is no longer in memory to avoid expensive disk I/O patterns.

## Parameters / Member Variables
- `winstate`: The WindowAggState containing execution state, including the tuplestore buffer, partition information, and execution status
- `pos`: The target position up to which tuples should be spooled, or -1 to spool the entire partition

## Dependencies
- Functions called/Symbols referenced:
  - outerPlanState
  - ExecProcNode
  - TupIsNull
  - tuplestore_in_memory
  - ExecQualAndReset
  - ExecCopySlot
  - tuplestore_puttupleslot
- Called from (representative examples):
  - update_frameheadpos
  - update_frametailpos
  - update_grouptailpos
  - ExecWindowAgg
  - window_gettupleslot
  - WinGetPartitionRowCount
  - WinGetFuncArgInPartition

## Notes and Other Information
- Contains a performance kluge that forces entire partition spooling when tuplestore spills to disk to avoid expensive alternating read/write patterns
- Handles three execution modes: WINDOWAGG_RUN, WINDOWAGG_PASSTHROUGH, and WINDOWAGG_PASSTHROUGH_STRICT
- In pass-through modes, may skip storing tuples in the tuplestore depending on whether the node is top-level
- Detects partition boundaries using partition equality functions when partNumCols > 0
- Properly manages memory contexts by switching to query context when calling the outer plan
- Updates spooled_rows counter and partition status flags as tuples are processed