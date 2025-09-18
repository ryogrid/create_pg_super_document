# ExecGetResultSlotOps

## Location
src/backend/executor/execUtils.c: 502 - 537

## Overview
Returns the tuple table slot operations structure for a plan state's result slot, providing information about the slot's storage type and whether it uses fixed-format tuples.

## Definition
```c
const TupleTableSlotOps *ExecGetResultSlotOps(PlanState *planstate, bool *isfixed)
```

## Detailed Description
ExecGetResultSlotOps retrieves the TupleTableSlotOps structure that defines the operations and storage characteristics for a plan node's result tuple slot. This function implements a caching mechanism where the slot operations and fixed-format flag are stored in the planstate for efficiency.

The function follows this logic:
1. If the planstate has cached result operations (resultopsset && resultops), return the cached operations
2. If no cached operations exist but a result tuple slot is present, return the slot's operations
3. If no result tuple slot exists, return virtual tuple table slot operations (TTSOpsVirtual)

The isfixed parameter is set to indicate whether the tuple slot uses a fixed tuple format, which affects performance optimizations in the executor.

## Parameters / Member Variables
- `planstate`: Pointer to the PlanState structure containing execution state for a plan node
- `isfixed`: Output parameter that receives whether the slot uses fixed-format tuples (can be NULL)

## Dependencies
- Functions called/Symbols referenced:
  - TTS_FIXED (macro to check if slot uses fixed format)
  - TTSOpsVirtual (default virtual tuple table slot operations)
- Called from (representative examples):
  - ExecComputeSlotInfo (execExpr.c:2934, 2954)
  - ExecInitAgg (nodeAgg.c:3311)
  - ExecInitGroup (nodeGroup.c:191)
  - ExecInitHashJoin (nodeHashjoin.c:768)
  - Various node initialization functions across executor nodes

## Notes and Other Information
- Implements caching to avoid repeated lookups of slot operations
- The fixed-format flag optimization helps with performance in tuple access patterns
- Returns virtual slot operations as a safe default when no result slot exists
- Critical for proper slot management and optimization in PostgreSQL's execution engine
- The caching mechanism (resultopsset, resultops, resultopsfixed) improves performance for repeated accesses