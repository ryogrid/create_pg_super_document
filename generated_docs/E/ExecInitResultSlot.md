# ExecInitResultSlot

## Location
[src/backend/executor/execTuples.c:1866-1885](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execTuples.c#L1866-L1885)

## Overview
Initializes the result tuple slot for a plan node, allocating a new TupleTableSlot and associating it with the node's result tuple descriptor.

## Definition

```c
void
ExecInitResultSlot(PlanState *planstate, const TupleTableSlotOps *tts_ops)
```
## Detailed Description
ExecInitResultSlot is a convenience function that sets up the result tuple slot for a given plan node. It allocates a new TupleTableSlot using ExecAllocTableSlot and assigns it to the planstate's ps_ResultTupleSlot field. The function also configures the tuple slot operations and tracks whether the result operations are fixed based on whether a result tuple descriptor is available. This is part of the tuple slot initialization infrastructure that supports PostgreSQL's execution engine.

## Parameters / Member Variables
- `*planstate`: Pointer to the PlanState structure that needs its result slot initialized
- `*tts_ops`: Pointer to TupleTableSlotOps structure defining the operations for the tuple slot
## Dependencies
- Functions called/Symbols referenced:
  - [ExecAllocTableSlot](ExecAllocTableSlot.md): Creates and adds the tuple slot to the tuple table
  - [TupleTableSlotOps](../T/TupleTableSlotOps.md): Structure defining slot operations
- Called from (representative examples):
  - [ExecInitResultTupleSlotTL](ExecInitResultTupleSlotTL.md): Higher-level initialization that calls this function
  - [ExecConditionalAssignProjectionInfo](ExecConditionalAssignProjectionInfo.md): Part of projection setup process
  - [ExecQualAndReset](ExecQualAndReset.md): Used in qualification and reset operations

## Notes and Other Information
- This is a convenience routine as part of the ExecInit{Result,Scan,Extra}TupleSlot family
- The function sets internal flags (resultopsfixed, resultopsset) to track the state of result operations
- The result slot is allocated from the execution state's tuple table (es_tupleTable)
- Located in src/backend/executor/execTuples.c:1866-1885

## Simplified Source

```c
void
ExecInitResultSlot(PlanState *planstate, const TupleTableSlotOps *tts_ops)
{
    // Allocate a new tuple slot from the tuple table
    TupleTableSlot *slot = ExecAllocTableSlot(&planstate->state->es_tupleTable,
                                             planstate->ps_ResultTupleDesc,
                                             tts_ops);

    // Assign the slot to the plan state
    planstate->ps_ResultTupleSlot = slot;

    // Set operation flags for result slot management
    planstate->resultopsfixed = (planstate->ps_ResultTupleDesc != NULL);
    planstate->resultops = tts_ops;
    planstate->resultopsset = true;
}
```