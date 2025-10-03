# agg_retrieve_direct

## Location
[src/backend/executor/nodeAgg.c:2194-2539](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeAgg.c#L2194-L2539)

## Overview
agg_retrieve_direct implements non-hashed aggregation processing for PostgreSQL, handling plain aggregation and sorted grouping by directly processing input tuples and managing grouping set boundaries.

## Definition
```c
static TupleTableSlot *
agg_retrieve_direct(AggState *aggstate)
```

## Detailed Description
agg_retrieve_direct is the core function for processing aggregates when not using hash-based grouping (AGG_PLAIN, AGG_SORTED strategies). It manages the complex logic for:

**Multi-phase Processing**: Handles multiple phases of aggregation, particularly for mixed aggregation strategies where it can switch between direct processing and hash table processing.

**Grouping Set Management**: Supports PostgreSQL's GROUPING SETS feature by tracking which grouping sets need to be projected and managing boundaries between different sets.

**Input Processing**: Fetches input tuples from the outer plan, detects group boundaries, and maintains the first tuple of each group for comparison purposes.

**Context Management**: Manages expression contexts for both per-tuple and per-group operations, ensuring proper cleanup and reset of aggregate states between groups.

The function implements a complex state machine that:
1. Determines which grouping sets need to be reset at boundaries
2. Checks for phase completion and transitions to next phase or mixed mode
3. Detects group boundaries by comparing consecutive tuples
4. Initializes and advances aggregate computations
5. Projects final results for each completed group

## Parameters / Member Variables
- `aggstate`: The AggState structure containing all execution state for the aggregate node

## Dependencies
- Functions called/Symbols referenced:
  - [ReScanExprContext](../R/ReScanExprContext.md)
  - [initialize_phase](../i/initialize_phase.md)
  - ResetTupleHashIterator
  - [select_current_set](../s/select_current_set.md)
  - [agg_retrieve_hash_table](agg_retrieve_hash_table.md)
  - [ExecQualAndReset](../E/ExecQualAndReset.md)
  - [fetch_input_tuple](../f/fetch_input_tuple.md)
  - TupIsNull
  - [ExecCopySlotHeapTuple](../E/ExecCopySlotHeapTuple.md)
  - [initialize_aggregates](../i/initialize_aggregates.md)
  - [ExecForceStoreHeapTuple](../E/ExecForceStoreHeapTuple.md)
  - [lookup_hash_entries](../l/lookup_hash_entries.md)
  - [advance_aggregates](advance_aggregates.md)
  - ResetExprContext
  - [hashagg_finish_initial_spills](../h/hashagg_finish_initial_spills.md)
  - [ExecQual](../E/ExecQual.md)
  - [prepare_projection_slot](../p/prepare_projection_slot.md)
  - [finalize_aggregates](../f/finalize_aggregates.md)
  - [project_aggregates](../p/project_aggregates.md)
- Called from (representative examples):
  - [ExecAgg](../E/ExecAgg.md) (for AGG_PLAIN and AGG_SORTED strategies)

## Notes and Other Information
- This function handles the most complex aggregation scenarios including grouping sets and multi-phase processing
- For mixed aggregation (AGG_MIXED), it can switch to hash table processing by calling agg_retrieve_hash_table
- The function maintains careful state tracking through aggstate->projected_set to handle grouping set boundaries
- [Group](../G/Group.md) boundary detection relies on equality functions stored in aggstate->phase->eqfunctions
- Input tuple processing includes special handling for empty input when grouping sets are involved
- The function supports interrupt checking through the main execution loop for long-running aggregations

## Simplified Source

```c
static TupleTableSlot *
agg_retrieve_direct(AggState *aggstate)
{
    // Get execution contexts and state info
    ExprContext *econtext = aggstate->ss.ps.ps_ExprContext;
    ExprContext *tmpcontext = aggstate->tmpcontext;
    TupleTableSlot *firstSlot = aggstate->ss.ss_ScanTupleSlot;
    bool hasGroupingSets = aggstate->phase->numsets > 0;
    int numGroupingSets = Max(aggstate->phase->numsets, 1);

    // Main processing loop - retrieve groups until done
    while (!aggstate->agg_done)
    {
        // Reset expression contexts for new group
        ReScanExprContext(econtext);

        // Determine how many grouping sets to reset
        int numReset = (aggstate->projected_set >= 0) ?
                      aggstate->projected_set + 1 : numGroupingSets;

        // Reset aggregate contexts
        for (int i = 0; i < numReset; i++) {
            ReScanExprContext(aggstate->aggcontexts[i]);
        }

        // Check for phase completion and transitions
        if (aggstate->input_done &&
            aggstate->projected_set >= (numGroupingSets - 1))
        {
            if (aggstate->current_phase < aggstate->numphases - 1) {
                // Move to next phase
                initialize_phase(aggstate, aggstate->current_phase + 1);
                // Reset state for new phase
                aggstate->input_done = false;
                aggstate->projected_set = -1;
            }
            else if (aggstate->aggstrategy == AGG_MIXED) {
                // Switch to hash table processing
                initialize_phase(aggstate, 0);
                aggstate->table_filled = true;
                return agg_retrieve_hash_table(aggstate);
            }
            else {
                aggstate->agg_done = true;
                break;
            }
        }

        // Handle grouping set projection logic
        if (should_project_grouping_set(aggstate, tmpcontext)) {
            aggstate->projected_set += 1;
        }
        else {
            // Start new group processing
            aggstate->projected_set = 0;

            // Get first tuple of new group if needed
            if (aggstate->grp_firstTuple == NULL) {
                TupleTableSlot *outerslot = fetch_input_tuple(aggstate);
                if (!TupIsNull(outerslot)) {
                    aggstate->grp_firstTuple = ExecCopySlotHeapTuple(outerslot);
                }
                else {
                    // Handle empty input
                    handle_empty_input(aggstate, hasGroupingSets);
                }
            }

            // Initialize aggregates for new group
            initialize_aggregates(aggstate, aggstate->pergroups, numReset);

            // Process input tuples for current group
            if (aggstate->grp_firstTuple != NULL) {
                process_group_tuples(aggstate, firstSlot, tmpcontext);
            }

            // Set up output context
            econtext->ecxt_outertuple = firstSlot;
        }

        // Finalize and project current group
        int currentSet = aggstate->projected_set;
        prepare_projection_slot(aggstate, econtext->ecxt_outertuple, currentSet);
        select_current_set(aggstate, currentSet, false);
        finalize_aggregates(aggstate, aggstate->peragg,
                          aggstate->pergroups[currentSet]);

        // Project result - continue if no row to project yet
        TupleTableSlot *result = project_aggregates(aggstate);
        if (result)
            return result;
    }

    return NULL; // No more groups
}

// Helper function: Check if we should project a grouping set
static bool
should_project_grouping_set(AggState *aggstate, ExprContext *tmpcontext)
{
    // Complex grouping set boundary detection logic
    // Returns true if current grouping set should be projected
    return (aggstate->input_done ||
            (grouping_set_boundary_detected(aggstate, tmpcontext)));
}

// Helper function: Handle empty input cases
static void
handle_empty_input(AggState *aggstate, bool hasGroupingSets)
{
    if (hasGroupingSets) {
        aggstate->input_done = true;
        // Skip grouping sets with size > 0 for empty input
        while (aggstate->phase->gset_lengths[aggstate->projected_set] > 0) {
            aggstate->projected_set += 1;
            if (aggstate->projected_set >= aggstate->phase->numsets)
                break;
        }
    }
    else {
        aggstate->agg_done = true;
    }
}

// Helper function: Process all tuples in current group
static void
process_group_tuples(AggState *aggstate, TupleTableSlot *firstSlot,
                    ExprContext *tmpcontext)
{
    // Store first tuple and set up processing
    ExecForceStoreHeapTuple(aggstate->grp_firstTuple, firstSlot, true);
    aggstate->grp_firstTuple = NULL;
    tmpcontext->ecxt_outertuple = firstSlot;

    // Process tuples until group boundary or end of input
    for (;;) {
        // Update hash tables if mixed aggregation
        if (aggstate->aggstrategy == AGG_MIXED &&
            aggstate->current_phase == 1) {
            lookup_hash_entries(aggstate);
        }

        // Advance aggregates with current tuple
        advance_aggregates(aggstate);
        ResetExprContext(tmpcontext);

        // Fetch next tuple
        TupleTableSlot *outerslot = fetch_input_tuple(aggstate);
        if (TupIsNull(outerslot)) {
            // End of input
            if (aggstate->aggstrategy == AGG_MIXED &&
                aggstate->current_phase == 1) {
                hashagg_finish_initial_spills(aggstate);
            }
            aggstate->input_done = true;
            break;
        }

        tmpcontext->ecxt_outertuple = outerslot;

        // Check for group boundary
        if (group_boundary_detected(aggstate, firstSlot, tmpcontext)) {
            aggstate->grp_firstTuple = ExecCopySlotHeapTuple(outerslot);
            break;
        }
    }
}
```