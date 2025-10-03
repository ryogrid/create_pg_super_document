# setop_retrieve_direct

## Location
[src/backend/executor/nodeSetOp.c:227-338](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeSetOp.c#L227-L338)

## Overview
setop_retrieve_direct implements the direct (non-hashed) strategy for set operations, processing sorted input tuples by grouping consecutive identical tuples and applying set operation logic.

## Definition

```c
static TupleTableSlot *
setop_retrieve_direct(SetOpState *setopstate)
```
## Detailed Description
This function implements the core logic for set operations when inputs are sorted and can be processed directly without hashing. It operates by:

1. **Group Detection**: Reads tuples from the outer plan and groups consecutive identical tuples together using equality comparison functions
2. **Tuple Counting**: For each group, counts occurrences and tracks tuple flags (used for distinguishing left vs right input in operations like EXCEPT)
3. **Output Determination**: Based on the set operation type and counts, determines how many copies of each group should be output
4. **State Management**: Maintains group boundaries by saving the first tuple of the next group when a boundary is crossed

The function processes one group at a time, scanning through all tuples in the current group before determining the output. It handles the transition between groups by preserving the first tuple of the next group for the subsequent iteration.

## Parameters / Member Variables
- `*setopstate`: Pointer to the SetOpState structure containing the execution state, including tuple storage, counting information, equality functions, and output control
## Dependencies
- Functions called/Symbols referenced:
  - outerPlanState (gets the outer plan state)
  - [ExecProcNode](../E/ExecProcNode.md) (executes outer plan to get next tuple) 
  - TupIsNull (checks if tuple slot is empty)
  - [ExecCopySlotHeapTuple](../E/ExecCopySlotHeapTuple.md) (creates heap tuple copy)
  - [ExecStoreHeapTuple](../E/ExecStoreHeapTuple.md) (stores tuple in slot)
  - [initialize_counts](../i/initialize_counts.md) (resets per-group counters)
  - [advance_counts](../a/advance_counts.md) (updates counters for a tuple)
  - [fetch_tuple_flag](../f/fetch_tuple_flag.md) (gets tuple's flag value)
  - [ExecQualAndReset](../E/ExecQualAndReset.md) (evaluates equality expression)
  - [set_output_count](set_output_count.md) (determines output count for group)
  - [ExecClearTuple](../E/ExecClearTuple.md) (clears tuple slot)
- Called from (representative examples):
  - [ExecSetOp](../E/ExecSetOp.md) (when using direct strategy)

## Notes and Other Information
- Used for sorted inputs where tuples can be grouped by consecutive scanning
- Relies on input being sorted by the set operation's grouping columns
- Handles group boundary detection through equality function evaluation
- Maintains efficiency by processing one group at a time rather than materializing all input
- Part of PostgreSQL's two-strategy approach for set operations (direct vs hashed)
- Returns NULL when no more groups are available for processing

## Simplified Source

```c
static TupleTableSlot *
setop_retrieve_direct(SetOpState *setopstate)
{
    PlanState *outerPlan;
    SetOpStatePerGroup pergroup;
    TupleTableSlot *outerslot;
    TupleTableSlot *resultTupleSlot;
    ExprContext *econtext = setopstate->ps.ps_ExprContext;

    // Get state info from node
    outerPlan = outerPlanState(setopstate);
    pergroup = (SetOpStatePerGroup) setopstate->pergroup;
    resultTupleSlot = setopstate->ps.ps_ResultTupleSlot;

    // Process groups until we find one to return
    while (!setopstate->setop_done) {
        // Get first tuple of new group if needed
        if (setopstate->grp_firstTuple == NULL) {
            outerslot = ExecProcNode(outerPlan);
            if (!TupIsNull(outerslot)) {
                setopstate->grp_firstTuple = ExecCopySlotHeapTuple(outerslot);
            } else {
                setopstate->setop_done = true;
                return NULL;
            }
        }

        // Store first tuple in result slot
        ExecStoreHeapTuple(setopstate->grp_firstTuple, resultTupleSlot, true);
        setopstate->grp_firstTuple = NULL;

        // Initialize and count first tuple
        initialize_counts(pergroup);
        advance_counts(pergroup, fetch_tuple_flag(setopstate, resultTupleSlot));

        // Scan remaining tuples in current group
        for (;;) {
            outerslot = ExecProcNode(outerPlan);
            if (TupIsNull(outerslot)) {
                setopstate->setop_done = true;
                break;
            }

            // Check for group boundary
            econtext->ecxt_outertuple = resultTupleSlot;
            econtext->ecxt_innertuple = outerslot;

            if (!ExecQualAndReset(setopstate->eqfunction, econtext)) {
                // Save first tuple of next group
                setopstate->grp_firstTuple = ExecCopySlotHeapTuple(outerslot);
                break;
            }

            // Count tuple in current group
            advance_counts(pergroup, fetch_tuple_flag(setopstate, outerslot));
        }

        // Determine if this group should produce output
        set_output_count(setopstate, pergroup);

        if (setopstate->numOutput > 0) {
            setopstate->numOutput--;
            return resultTupleSlot;
        }
    }

    // No more groups
    ExecClearTuple(resultTupleSlot);
    return NULL;
}
```