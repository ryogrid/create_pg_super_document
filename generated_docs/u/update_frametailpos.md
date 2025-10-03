# update_frametailpos

## Location
[src/backend/executor/nodeWindowAgg.c:1735-1984](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeWindowAgg.c#L1735-L1984)

## Overview
A static function that computes and sets the frame tail position for the current row in window function processing, determining where the window frame ends based on various boundary modes including UNBOUNDED FOLLOWING, CURRENT ROW, and offset-based frames.

## Definition

```c
static void
update_frametailpos(WindowAggState *winstate)
```
## Detailed Description
This function is the counterpart to update_frameheadpos and is responsible for determining where the window frame ends relative to the current row being processed in window aggregation operations. The frame tail position represents the first row *after* the last row that should be included in the frame.

The function handles all supported frame end boundary types:
- **UNBOUNDED FOLLOWING**: Frame extends to the end of the partition
- **CURRENT ROW**: Frame ends at current row or last peer depending on frame mode  
- **OFFSET-based frames**: Frame ends at a specific offset from current row

Similar to frame head processing, the behavior varies by frame mode:
- **ROWS mode**: Operates on physical row positions with direct offset calculations
- **RANGE mode**: Uses ORDER BY column values and in_range functions to determine logical boundaries
- **GROUPS mode**: Works with peer groups where rows having identical ORDER BY values are treated as a single unit

The function maintains frame tail tracking through  and , ensures proper tuple spooling when accessing future rows, and uses memory context management for efficiency.

## Parameters / Member Variables
- : WindowAggState pointer containing all window function execution state including:

## Dependencies
- Functions called/Symbols referenced:
  - [spool_tuples](../s/spool_tuples.md) (tuple buffering to ensure data availability)
  - [MemoryContextSwitchTo](../M/MemoryContextSwitchTo.md) (memory management)
  - [tuplestore_select_read_pointer](../t/tuplestore_select_read_pointer.md) (tuple store navigation)
  - [tuplestore_gettupleslot](../t/tuplestore_gettupleslot.md) (tuple retrieval from buffer)
  - [are_peers](../a/are_peers.md) (peer comparison for RANGE/GROUPS modes)
  - [slot_getattr](../s/slot_getattr.md) (tuple attribute extraction for RANGE mode)
  - [FunctionCall5Coll](../F/FunctionCall5Coll.md) (in_range function calls for RANGE mode)
  - [ExecCopySlot](../E/ExecCopySlot.md)/ExecClearTuple (tuple slot management in GROUPS mode)
  - [DatumGetInt64](../D/DatumGetInt64.md) (offset value extraction)
- Called from (representative examples):
  - [row_is_in_frame](../r/row_is_in_frame.md) (for frame membership testing)
  - [ExecWindowAgg](../E/ExecWindowAgg.md) (main window aggregation execution)
  - [WinGetFuncArgInFrame](../W/WinGetFuncArgInFrame.md) (window function argument retrieval)

## Notes and Other Information
- The function may clobber  during GROUPS mode processing
- Frame tail position computation ignores window exclusion clauses - exclusion is applied separately
- Uses memory context switching to prevent accumulation of temporary allocations
- Maintains frame tail validity flags to avoid redundant computation for the same row
- For RANGE mode, requires exactly one ORDER BY column and uses specialized in_range functions
- Handles NULL values in ORDER BY columns according to NULLS FIRST/LAST specifications
- Frame tail position is always one past the last included row (exclusive end boundary)
- Critical for performance as it determines the ending boundary for all frame-based window function computations

## Simplified Source

```c
static void
update_frametailpos(WindowAggState *winstate)
{
    WindowAgg *node = (WindowAgg *) winstate->ss.ps.plan;
    int frameOptions = winstate->frameOptions;

    // Return if position already computed for current row
    if (winstate->frametail_valid)
        return;

    // Switch to persistent memory context
    MemoryContext oldcontext = MemoryContextSwitchTo(
        winstate->ss.ps.ps_ExprContext->ecxt_per_query_memory);

    if (frameOptions & FRAMEOPTION_END_UNBOUNDED_FOLLOWING) {
        // Frame extends to end of partition
        spool_tuples(winstate, -1);
        winstate->frametailpos = winstate->spooled_rows;
        winstate->frametail_valid = true;
    }
    else if (frameOptions & FRAMEOPTION_END_CURRENT_ROW) {
        if (frameOptions & FRAMEOPTION_ROWS) {
            // In ROWS mode, tail is one past current row
            winstate->frametailpos = winstate->currentpos + 1;
            winstate->frametail_valid = true;
        }
        else if (frameOptions & (FRAMEOPTION_RANGE | FRAMEOPTION_GROUPS)) {
            // If no ORDER BY, all rows are peers
            if (node->ordNumCols == 0) {
                spool_tuples(winstate, -1);
                winstate->frametailpos = winstate->spooled_rows;
                winstate->frametail_valid = true;
                MemoryContextSwitchTo(oldcontext);
                return;
            }

            // Find first row after current peer group
            tuplestore_select_read_pointer(winstate->buffer, winstate->frametail_ptr);

            // Initialize frame tail slot if needed
            if (winstate->frametailpos == 0 && TupIsNull(winstate->frametail_slot)) {
                tuplestore_gettupleslot(winstate->buffer, true, true, winstate->frametail_slot);
            }

            // Advance until we find non-peer of current row
            while (!TupIsNull(winstate->frametail_slot)) {
                if (winstate->frametailpos > winstate->currentpos &&
                    !are_peers(winstate, winstate->frametail_slot, winstate->ss.ss_ScanTupleSlot))
                    break;

                winstate->frametailpos++;
                spool_tuples(winstate, winstate->frametailpos);
                if (!tuplestore_gettupleslot(winstate->buffer, true, true, winstate->frametail_slot))
                    break;
            }
            winstate->frametail_valid = true;
        }
    }
    else if (frameOptions & FRAMEOPTION_END_OFFSET) {
        if (frameOptions & FRAMEOPTION_ROWS) {
            // Calculate physical offset from current row
            int64 offset = DatumGetInt64(winstate->endOffsetValue);
            if (frameOptions & FRAMEOPTION_END_OFFSET_PRECEDING)
                offset = -offset;

            winstate->frametailpos = winstate->currentpos + offset + 1;

            // Ensure position stays within partition bounds
            if (winstate->frametailpos < 0)
                winstate->frametailpos = 0;
            else if (winstate->frametailpos > winstate->currentpos + 1) {
                spool_tuples(winstate, winstate->frametailpos - 1);
                if (winstate->frametailpos > winstate->spooled_rows)
                    winstate->frametailpos = winstate->spooled_rows;
            }
            winstate->frametail_valid = true;
        }
        else if (frameOptions & FRAMEOPTION_RANGE) {
            // Use in_range function for RANGE offset mode
            int sortCol = node->ordColIdx[0];
            bool sub = (frameOptions & FRAMEOPTION_END_OFFSET_PRECEDING);
            bool less = true;

            // Flip flags if descending sort order
            if (!winstate->inRangeAsc) {
                sub = !sub;
                less = false;
            }

            tuplestore_select_read_pointer(winstate->buffer, winstate->frametail_ptr);

            // Initialize if needed
            if (winstate->frametailpos == 0 && TupIsNull(winstate->frametail_slot)) {
                tuplestore_gettupleslot(winstate->buffer, true, true, winstate->frametail_slot);
            }

            // Find first row beyond in_range constraint
            while (!TupIsNull(winstate->frametail_slot)) {
                Datum tailval = slot_getattr(winstate->frametail_slot, sortCol, &tailisnull);
                Datum currval = slot_getattr(winstate->ss.ss_ScanTupleSlot, sortCol, &currisnull);

                // Handle NULL values or call in_range function
                if (!tailisnull && !currisnull) {
                    if (!DatumGetBool(FunctionCall5Coll(&winstate->endInRangeFunc,
                                                        winstate->inRangeColl,
                                                        tailval, currval,
                                                        winstate->endOffsetValue,
                                                        BoolGetDatum(sub),
                                                        BoolGetDatum(less))))
                        break;
                }

                winstate->frametailpos++;
                spool_tuples(winstate, winstate->frametailpos);
                if (!tuplestore_gettupleslot(winstate->buffer, true, true, winstate->frametail_slot))
                    break;
            }
            winstate->frametail_valid = true;
        }
        else if (frameOptions & FRAMEOPTION_GROUPS) {
            // Groups mode: find first row beyond target peer group
            int64 offset = DatumGetInt64(winstate->endOffsetValue);
            int64 max_group = (frameOptions & FRAMEOPTION_END_OFFSET_PRECEDING)
                            ? winstate->currentgroup - offset
                            : winstate->currentgroup + offset;

            tuplestore_select_read_pointer(winstate->buffer, winstate->frametail_ptr);

            // Initialize if needed
            if (winstate->frametailpos == 0 && TupIsNull(winstate->frametail_slot)) {
                tuplestore_gettupleslot(winstate->buffer, true, true, winstate->frametail_slot);
            }

            // Advance beyond target group
            while (!TupIsNull(winstate->frametail_slot)) {
                if (winstate->frametailgroup > max_group)
                    break;

                ExecCopySlot(winstate->temp_slot_2, winstate->frametail_slot);
                winstate->frametailpos++;
                spool_tuples(winstate, winstate->frametailpos);

                if (!tuplestore_gettupleslot(winstate->buffer, true, true, winstate->frametail_slot))
                    break;

                if (!are_peers(winstate, winstate->temp_slot_2, winstate->frametail_slot))
                    winstate->frametailgroup++;
            }
            ExecClearTuple(winstate->temp_slot_2);
            winstate->frametail_valid = true;
        }
    }

    MemoryContextSwitchTo(oldcontext);
}
```