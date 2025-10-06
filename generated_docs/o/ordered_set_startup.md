# ordered_set_startup

## Location
[src/backend/utils/adt/orderedsetaggs.c:113-338](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/orderedsetaggs.c#L113-L338)

## Overview
Sets up working state for an ordered-set aggregate function, initializing per-query and per-group state structures required for sorting and managing aggregated data.

## Definition

```c
structures in the fn_mcxt, which we assume is the
		 * executor's per-query context;
```
## Detailed Description
The  function initializes the necessary state structures for ordered-set aggregate functions like , , and . It performs both per-query initialization (done once per query) and per-group initialization (done once per aggregate group).

The function first validates that it's being called in an aggregate context, then sets up a per-query state structure if it doesn't already exist. This includes analyzing the aggregate's sort requirements and preparing tuple descriptors or datum sorting information. Finally, it creates a per-group state structure with an initialized tuplesort object for collecting and sorting the aggregated values.

For hypothetical-set aggregates, it adds a special flag column to distinguish between regular input rows and the hypothetical row. The function supports both tuple-based sorting (for complex aggregates with multiple columns) and datum-based sorting (for simple single-column aggregates).

## Parameters / Member Variables
- : Function call information containing aggregate context and metadata
- : Boolean flag indicating whether to use tuple-based sorting (true) or datum-based sorting (false)

## Dependencies
- Functions called/Symbols referenced:
  - [AggCheckCallContext](../A/AggCheckCallContext.md)
  - [AggGetAggref](../A/AggGetAggref.md)
  - [AggStateIsShared](../A/AggStateIsShared.md)
  - [AggRegisterCallback](../A/AggRegisterCallback.md)
  - [tuplesort_begin_heap](../t/tuplesort_begin_heap.md)
  - [tuplesort_begin_datum](../t/tuplesort_begin_datum.md)
  - [ExecTypeFromTL](../E/ExecTypeFromTL.md)
  - [get_sortgroupclause_tle](../g/get_sortgroupclause_tle.md)
  - [MakeSingleTupleTableSlot](../M/MakeSingleTupleTableSlot.md)
  - [ordered_set_shutdown](ordered_set_shutdown.md)
- Called from (representative examples):
  - [ordered_set_transition](ordered_set_transition.md)
  - [ordered_set_transition_multi](ordered_set_transition_multi.md)

## Notes and Other Information
- The function maintains two levels of state: per-query state (cached in ) and per-group state (allocated in group-lifespan memory context)
- Supports rescanning if the aggregate state is shared across multiple execution nodes
- Registers a shutdown callback to clean up resources at the end of each group
- Handles both regular ordered-set aggregates and hypothetical-set aggregates with special flag column logic
- Uses  to configure the tuplesort memory usage limit

## Simplified Source

```c
static OSAPerGroupState *
ordered_set_startup(FunctionCallInfo fcinfo, bool use_tuples)
{
    OSAPerGroupState *osastate;
    OSAPerQueryState *qstate;
    MemoryContext gcontext;

    // Verify we're called in aggregate context
    if (AggCheckCallContext(fcinfo, &gcontext) != AGG_CONTEXT_AGGREGATE)
        elog(ERROR, "ordered-set aggregate called in non-aggregate context");

    // Get or create per-query state
    qstate = (OSAPerQueryState *) fcinfo->flinfo->fn_extra;
    if (qstate == NULL) {
        // First time setup - initialize per-query structures
        Aggref *aggref = AggGetAggref(fcinfo);
        MemoryContext qcontext = fcinfo->flinfo->fn_mcxt;

        qstate = (OSAPerQueryState *) palloc0(sizeof(OSAPerQueryState));
        qstate->aggref = aggref;
        qstate->rescan_needed = AggStateIsShared(fcinfo);

        // Extract sort information from aggregate definition
        List *sortlist = aggref->aggorder;
        int numSortCols = list_length(sortlist);

        if (use_tuples) {
            // Setup tuple-based sorting for multi-column aggregates
            qstate->numSortCols = numSortCols;
            qstate->tupdesc = ExecTypeFromTL(aggref->args);
            qstate->tupslot = MakeSingleTupleTableSlot(qstate->tupdesc, &TTSOpsMinimalTuple);
            // ... setup sort operators and column info
        } else {
            // Setup datum-based sorting for single-column aggregates
            SortGroupClause *sortcl = (SortGroupClause *) linitial(sortlist);
            TargetEntry *tle = get_sortgroupclause_tle(sortcl, aggref->args);

            qstate->sortColType = exprType((Node *) tle->expr);
            qstate->sortOperator = sortcl->sortop;
            // ... save other sort info
        }

        fcinfo->flinfo->fn_extra = (void *) qstate;
    }

    // Create per-group state structure
    osastate = (OSAPerGroupState *) palloc(sizeof(OSAPerGroupState));
    osastate->qstate = qstate;

    // Initialize tuplesort object for collecting sorted data
    if (use_tuples)
        osastate->sortstate = tuplesort_begin_heap(/* tuple sort parameters */);
    else
        osastate->sortstate = tuplesort_begin_datum(/* datum sort parameters */);

    osastate->number_of_rows = 0;
    osastate->sort_done = false;

    // Register cleanup callback
    AggRegisterCallback(fcinfo, ordered_set_shutdown, PointerGetDatum(osastate));

    return osastate;
}
```