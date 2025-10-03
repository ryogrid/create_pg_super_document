# spool_tuples

## Location
[src/backend/executor/nodeWindowAgg.c:1241-1334](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeWindowAgg.c#L1241-L1334)

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
  - [ExecProcNode](../E/ExecProcNode.md)
  - TupIsNull
  - [tuplestore_in_memory](../t/tuplestore_in_memory.md)
  - [ExecQualAndReset](../E/ExecQualAndReset.md)
  - [ExecCopySlot](../E/ExecCopySlot.md)
  - [tuplestore_puttupleslot](../t/tuplestore_puttupleslot.md)
- Called from (representative examples):
  - [update_frameheadpos](../u/update_frameheadpos.md)
  - [update_frametailpos](../u/update_frametailpos.md)
  - [update_grouptailpos](../u/update_grouptailpos.md)
  - [ExecWindowAgg](../E/ExecWindowAgg.md)
  - [window_gettupleslot](../w/window_gettupleslot.md)
  - [WinGetPartitionRowCount](../W/WinGetPartitionRowCount.md)
  - [WinGetFuncArgInPartition](../W/WinGetFuncArgInPartition.md)

## Notes and Other Information
- Contains a performance kluge that forces entire partition spooling when tuplestore spills to disk to avoid expensive alternating read/write patterns
- Handles three execution modes: WINDOWAGG_RUN, WINDOWAGG_PASSTHROUGH, and WINDOWAGG_PASSTHROUGH_STRICT
- In pass-through modes, may skip storing tuples in the tuplestore depending on whether the node is top-level
- Detects partition boundaries using partition equality functions when partNumCols > 0
- Properly manages memory contexts by switching to query context when calling the outer plan

## Simplified Source

```c
static void
spool_tuples(WindowAggState *winstate, int64 pos)
{
    WindowAgg *node = (WindowAgg *) winstate->ss.ps.plan;
    PlanState *outerPlan;
    TupleTableSlot *outerslot;

    // Safety checks and early exits
    if (!winstate->buffer)
        return;
    if (winstate->partition_spooled)
        return;

    // In pass-through mode or if tuplestore spilled to disk, spool entire partition
    if (winstate->status != WINDOWAGG_RUN || !tuplestore_in_memory(winstate->buffer))
        pos = -1;

    outerPlan = outerPlanState(winstate);

    // Switch to query memory context for calling outer plan
    MemoryContext oldcontext = MemoryContextSwitchTo(
        winstate->ss.ps.ps_ExprContext->ecxt_per_query_memory);

    // Read tuples until we reach target position or end of partition
    while (winstate->spooled_rows <= pos || pos == -1) {
        outerslot = ExecProcNode(outerPlan);
        if (TupIsNull(outerslot)) {
            // End of input
            winstate->partition_spooled = true;
            winstate->more_partitions = false;
            break;
        }

        // Check for partition boundary if partitioning is used
        if (node->partNumCols > 0) {
            ExprContext *econtext = winstate->tmpcontext;
            econtext->ecxt_innertuple = winstate->first_part_slot;
            econtext->ecxt_outertuple = outerslot;

            // Test if tuple belongs to current partition
            if (!ExecQualAndReset(winstate->partEqfunction, econtext)) {
                // New partition starts - save first tuple of next partition
                ExecCopySlot(winstate->first_part_slot, outerslot);
                winstate->partition_spooled = true;
                winstate->more_partitions = true;
                break;
            }
        }

        // Store tuple in tuplestore (unless in strict pass-through mode)
        if (winstate->status != WINDOWAGG_PASSTHROUGH_STRICT) {
            tuplestore_puttupleslot(winstate->buffer, outerslot);
            winstate->spooled_rows++;
        }
    }

    MemoryContextSwitchTo(oldcontext);
}
```
- Updates spooled_rows counter and partition status flags as tuples are processed