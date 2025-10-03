# begin_partition

## Location
[src/backend/executor/nodeWindowAgg.c:1081-1240](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeWindowAgg.c#L1081-L1240)

## Overview
This static function initializes and sets up the buffering infrastructure for processing rows of the next window partition, including creating tuplestores and read pointers for various window functions.

## Definition
```c
static void begin_partition(WindowAggState *winstate)
```

## Detailed Description
The `begin_partition` function prepares the WindowAgg execution state to begin processing a new partition of input rows. It initializes all position tracking variables, clears tuple slots, creates a new tuplestore for buffering partition data, and sets up read pointers required by different types of window functions and frame specifications.

The function handles the complex setup required for different frame options (RANGE, GROUPS, ROWS), exclusion clauses (EXCLUDE GROUP, EXCLUDE TIES), and aggregate functions. It creates specialized read pointers with appropriate capabilities (BACKWARD seeking) based on the frame specification and window function requirements.

For the very first partition, it fetches the initial input row from the outer plan. The function also stores the first tuple of the partition into the newly created tuplestore and updates the spooled row counter.

## Parameters / Member Variables
- `winstate`: The WindowAggState containing all state information for window aggregation processing, including frame options, function definitions, and position tracking

## Dependencies
- Functions called/Symbols referenced:
  - outerPlanState
  - [ExecClearTuple](../E/ExecClearTuple.md)
  - [ExecProcNode](../E/ExecProcNode.md)
  - [ExecCopySlot](../E/ExecCopySlot.md)
  - TupIsNull
  - [tuplestore_begin_heap](../t/tuplestore_begin_heap.md)
  - [tuplestore_set_eflags](../t/tuplestore_set_eflags.md)
  - [tuplestore_alloc_read_pointer](../t/tuplestore_alloc_read_pointer.md)
  - [tuplestore_puttupleslot](../t/tuplestore_puttupleslot.md)
- Called from (representative examples):
  - [ExecWindowAgg](../E/ExecWindowAgg.md)

## Notes and Other Information
- Resets all position tracking variables (currentpos, frameheadpos, frametailpos, etc.) to their initial states
- Creates read pointers conditionally based on frame options - RANGE/GROUPS modes may need special pointers for frame boundary access
- Handles aggregate functions by setting up mark/read pointers with BACKWARD capability when frame head is movable
- Sets up specialized read pointers for peer group tracking when exclusion clauses (EXCLUDE GROUP/TIES) are present
- The function assumes work_mem is available globally for tuplestore creation
- Manages memory efficiently by only creating the read pointers that are actually needed based on the window specification

## Simplified Source

```c
static void
begin_partition(WindowAggState *winstate)
{
    WindowAgg *node = (WindowAgg *) winstate->ss.ps.plan;
    PlanState *outerPlan = outerPlanState(winstate);
    int frameOptions = winstate->frameOptions;
    int numfuncs = winstate->numfuncs;

    // Initialize all position tracking variables
    winstate->partition_spooled = false;
    winstate->framehead_valid = false;
    winstate->frametail_valid = false;
    winstate->grouptail_valid = false;
    winstate->spooled_rows = 0;
    winstate->currentpos = 0;
    winstate->frameheadpos = 0;
    winstate->frametailpos = 0;
    winstate->currentgroup = 0;
    winstate->frameheadgroup = 0;
    winstate->frametailgroup = 0;
    winstate->groupheadpos = 0;
    winstate->grouptailpos = -1;

    // Clear all tuple slots
    ExecClearTuple(winstate->agg_row_slot);
    if (winstate->framehead_slot)
        ExecClearTuple(winstate->framehead_slot);
    if (winstate->frametail_slot)
        ExecClearTuple(winstate->frametail_slot);

    // For the first partition, fetch the initial input row
    if (TupIsNull(winstate->first_part_slot)) {
        TupleTableSlot *outerslot = ExecProcNode(outerPlan);
        if (!TupIsNull(outerslot))
            ExecCopySlot(winstate->first_part_slot, outerslot);
        else {
            // No input data
            winstate->partition_spooled = true;
            winstate->more_partitions = false;
            return;
        }
    }

    // Create new tuplestore for this partition
    winstate->buffer = tuplestore_begin_heap(false, false, work_mem);

    // Set up basic read pointer
    winstate->current_ptr = 0;
    tuplestore_set_eflags(winstate->buffer, 0);

    // Create read pointers for aggregates if needed
    if (winstate->numaggs > 0) {
        WindowObject agg_winobj = winstate->agg_winobj;
        int readptr_flags = 0;

        // Create mark pointer if frame head can move or exclusion is used
        if (!(frameOptions & FRAMEOPTION_START_UNBOUNDED_PRECEDING) ||
            (frameOptions & FRAMEOPTION_EXCLUSION)) {
            agg_winobj->markptr = tuplestore_alloc_read_pointer(winstate->buffer, 0);
            readptr_flags |= EXEC_FLAG_BACKWARD;
        }

        agg_winobj->readptr = tuplestore_alloc_read_pointer(winstate->buffer, readptr_flags);
        agg_winobj->markpos = -1;
        agg_winobj->seekpos = -1;

        // Reset aggregate row counters
        winstate->aggregatedbase = 0;
        winstate->aggregatedupto = 0;
    }

    // Create read pointers for window functions
    for (int i = 0; i < numfuncs; i++) {
        WindowStatePerFunc perfuncstate = &(winstate->perfunc[i]);
        if (!perfuncstate->plain_agg) {
            WindowObject winobj = perfuncstate->winobj;
            winobj->markptr = tuplestore_alloc_read_pointer(winstate->buffer, 0);
            winobj->readptr = tuplestore_alloc_read_pointer(winstate->buffer, EXEC_FLAG_BACKWARD);
            winobj->markpos = -1;
            winobj->seekpos = -1;
        }
    }

    // Create frame boundary read pointers for RANGE/GROUPS modes
    winstate->framehead_ptr = winstate->frametail_ptr = -1;
    if (frameOptions & (FRAMEOPTION_RANGE | FRAMEOPTION_GROUPS)) {
        if (((frameOptions & FRAMEOPTION_START_CURRENT_ROW) && node->ordNumCols != 0) ||
            (frameOptions & FRAMEOPTION_START_OFFSET))
            winstate->framehead_ptr = tuplestore_alloc_read_pointer(winstate->buffer, 0);

        if (((frameOptions & FRAMEOPTION_END_CURRENT_ROW) && node->ordNumCols != 0) ||
            (frameOptions & FRAMEOPTION_END_OFFSET))
            winstate->frametail_ptr = tuplestore_alloc_read_pointer(winstate->buffer, 0);
    }

    // Create group tail pointer for exclusion clauses
    winstate->grouptail_ptr = -1;
    if ((frameOptions & (FRAMEOPTION_EXCLUDE_GROUP | FRAMEOPTION_EXCLUDE_TIES)) &&
        node->ordNumCols != 0) {
        winstate->grouptail_ptr = tuplestore_alloc_read_pointer(winstate->buffer, 0);
    }

    // Store the first tuple and increment counter
    tuplestore_puttupleslot(winstate->buffer, winstate->first_part_slot);
    winstate->spooled_rows++;
}
```