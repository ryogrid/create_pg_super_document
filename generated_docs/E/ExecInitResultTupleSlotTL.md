# ExecInitResultTupleSlotTL

## Location
src/backend/executor/execTuples.c: 1886 - 1897

## Overview
A high-level convenience function that initializes both the result tuple descriptor and result tuple slot for a plan node using the plan's target list.

## Definition
```c
void ExecInitResultTupleSlotTL(PlanState *planstate, const TupleTableSlotOps *tts_ops)
```

## Detailed Description
ExecInitResultTupleSlotTL provides a complete initialization sequence for a plan node's result handling infrastructure. It first calls ExecInitResultTypeTL to set up the result tuple descriptor based on the plan node's target list, then calls ExecInitResultSlot to allocate and initialize the actual tuple slot. This two-step process ensures that the tuple slot has the correct type information derived from the plan's output specifications. This function is widely used across PostgreSQL's executor nodes that produce results based on their target lists.

## Parameters / Member Variables
- `planstate`: Pointer to the PlanState structure that needs its result slot and type initialized
- `tts_ops`: Pointer to TupleTableSlotOps structure defining the operations for the tuple slot

## Dependencies
- Functions called/Symbols referenced:
  - ExecInitResultTypeTL: Initializes the result tuple descriptor from the target list
  - ExecInitResultSlot: Allocates and initializes the result tuple slot
  - TupleTableSlotOps: Structure defining slot operations
- Called from (representative examples):
  - ExecInitAgg: Aggregation node initialization
  - ExecInitAppend: Append node initialization
  - ExecInitHash: Hash node initialization
  - ExecInitHashJoin: Hash join node initialization
  - ExecInitMergeJoin: Merge join node initialization
  - ExecInitSort: Sort node initialization
  - And many other executor node initialization functions

## Notes and Other Information
- This is the most commonly used initialization function for executor nodes that produce results
- Combines tuple descriptor and slot initialization in a single convenient call
- The 'TL' suffix indicates it uses the Target List approach for determining result structure
- Used extensively across different types of executor nodes (joins, sorts, aggregates, etc.)
- Located in src/backend/executor/execTuples.c:1886-1897