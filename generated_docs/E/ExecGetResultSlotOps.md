# ExecGetResultSlotOps

## Location
[src/backend/executor/execUtils.c:502-537](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execUtils.c#L502-L537)

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
  - [ExecComputeSlotInfo](ExecComputeSlotInfo.md) (execExpr.c:2934, 2954)
  - [ExecInitAgg](ExecInitAgg.md) (nodeAgg.c:3311)
  - [ExecInitGroup](ExecInitGroup.md) (nodeGroup.c:191)
  - [ExecInitHashJoin](ExecInitHashJoin.md) (nodeHashjoin.c:768)
  - Various node initialization functions across executor nodes

## Notes and Other Information
- Implements caching to avoid repeated lookups of slot operations
- The fixed-format flag optimization helps with performance in tuple access patterns
- Returns virtual slot operations as a safe default when no result slot exists
- Critical for proper slot management and optimization in PostgreSQL's execution engine
- The caching mechanism (resultopsset, resultops, resultopsfixed) improves performance for repeated accesses

## Simplified Source

```c
const TupleTableSlotOps *ExecGetResultSlotOps(PlanState *planstate, bool *isfixed) {
    // Return cached operations if available
    if (planstate->resultopsset && planstate->resultops) {
        if (isfixed)
            *isfixed = planstate->resultopsfixed;
        return planstate->resultops;
    }

    // Set isfixed output parameter based on available information
    if (isfixed) {
        if (planstate->resultopsset)
            *isfixed = planstate->resultopsfixed;
        else if (planstate->ps_ResultTupleSlot)
            *isfixed = TTS_FIXED(planstate->ps_ResultTupleSlot);
        else
            *isfixed = false;
    }

    // Return slot operations or virtual default
    if (!planstate->ps_ResultTupleSlot)
        return &TTSOpsVirtual;

    return planstate->ps_ResultTupleSlot->tts_ops;
}
```