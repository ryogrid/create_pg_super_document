# ExecInitIncrementalSort

## Location
[src/backend/executor/nodeIncrementalSort.c:976-1076](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeIncrementalSort.c#L976-L1076)

## Overview
Initializes the runtime state for an incremental sort node, creating the necessary data structures and setting up the outer child node for execution.

## Definition

```c
structure. */
	incrsortstate = makeNode(IncrementalSortState);
```
## Detailed Description
ExecInitIncrementalSort creates and initializes the runtime state information for an incremental sort node produced by the planner. This function sets up the IncrementalSortState structure with initial values, validates execution flags to ensure compatibility with incremental sorting limitations, and initializes both the outer child node and necessary tuple slots.

The function performs several critical initialization tasks:
- Creates and initializes the IncrementalSortState structure
- Sets execution status to INCSORT_LOADFULLSORT (initial state)
- Validates that incompatible flags (EXEC_FLAG_BACKWARD, EXEC_FLAG_MARK) are not set
- Initializes instrumentation data structures for performance monitoring
- Sets up scan and result tuple slots using minimal tuple operations
- Creates standalone slots for pivot prefix keys and tuple transfer between batches

Incremental sort cannot support backward scans or mark/restore operations because it maintains only the current sort batch rather than the complete result set.

## Parameters / Member Variables
- : The IncrementalSort plan node containing sort specifications and configuration
- : The execution state containing global execution context and memory management
- : Execution flags that control scan behavior and capabilities

## Dependencies
- Functions called/Symbols referenced:
  - makeNode (creates IncrementalSortState)
  - [ExecInitNode](ExecInitNode.md) (initializes outer child node)
  - [ExecCreateScanSlotFromOuterPlan](ExecCreateScanSlotFromOuterPlan.md) (creates scan slot)
  - [ExecInitResultTupleSlotTL](ExecInitResultTupleSlotTL.md) (initializes result slot and tuple descriptor)
  - [MakeSingleTupleTableSlot](../M/MakeSingleTupleTableSlot.md) (creates standalone tuple slots)
  - [ExecGetResultType](ExecGetResultType.md) (gets result tuple descriptor)
  - SO_printf (debug output)
- Called from (representative examples):
  - [ExecInitNode](ExecInitNode.md) (main node initialization dispatcher)

## Notes and Other Information
- Incremental sort is incompatible with EXEC_FLAG_BACKWARD and EXEC_FLAG_MARK flags
- The function initializes instrumentation structures for both fullsort and prefixsort operations to track performance metrics
- Two standalone tuple slots are created: one for storing pivot prefix keys (group_pivot) and another for carrying tuples between batches (transfer_tuple)
- The execution status is initially set to INCSORT_LOADFULLSORT, indicating the first phase of incremental sorting
- No projection information is needed since incremental sort nodes don't perform projections

## Simplified Source

```c
IncrementalSortState *
ExecInitIncrementalSort(IncrementalSort *node, EState *estate, int eflags)
{
    IncrementalSortState *incrsortstate;

    // Validate execution flags - incremental sort doesn't support backward scans or mark/restore
    Assert((eflags & (EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK)) == 0);

    // Create and initialize state structure
    incrsortstate = makeNode(IncrementalSortState);
    incrsortstate->ss.ps.plan = (Plan *) node;
    incrsortstate->ss.ps.state = estate;
    incrsortstate->ss.ps.ExecProcNode = ExecIncrementalSort;

    // Initialize execution state variables
    incrsortstate->execution_status = INCSORT_LOADFULLSORT;
    incrsortstate->bounded = false;
    incrsortstate->outerNodeDone = false;
    incrsortstate->bound_Done = 0;
    incrsortstate->fullsort_state = NULL;
    incrsortstate->prefixsort_state = NULL;
    incrsortstate->group_pivot = NULL;
    incrsortstate->transfer_tuple = NULL;
    incrsortstate->n_fullsort_remaining = 0;
    incrsortstate->presorted_keys = NULL;

    // Initialize instrumentation for performance monitoring if enabled
    if (incrsortstate->ss.ps.instrument != NULL)
    {
        // Zero out fullsort and prefixsort group info counters
        memset(&incrsortstate->incsort_info.fullsortGroupInfo, 0,
               sizeof(IncrementalSortGroupInfo));
        memset(&incrsortstate->incsort_info.prefixsortGroupInfo, 0,
               sizeof(IncrementalSortGroupInfo));
    }

    // Initialize child node (outer plan)
    outerPlanState(incrsortstate) = ExecInitNode(outerPlan(node), estate, eflags);

    // Set up scan slot and result slot
    ExecCreateScanSlotFromOuterPlan(estate, &incrsortstate->ss, &TTSOpsMinimalTuple);
    ExecInitResultTupleSlotTL(&incrsortstate->ss.ps, &TTSOpsMinimalTuple);
    incrsortstate->ss.ps.ps_ProjInfo = NULL;

    // Create standalone slots for pivot keys and tuple transfer
    incrsortstate->group_pivot =
        MakeSingleTupleTableSlot(ExecGetResultType(outerPlanState(incrsortstate)),
                                &TTSOpsMinimalTuple);
    incrsortstate->transfer_tuple =
        MakeSingleTupleTableSlot(ExecGetResultType(outerPlanState(incrsortstate)),
                                &TTSOpsMinimalTuple);

    return incrsortstate;
}
```