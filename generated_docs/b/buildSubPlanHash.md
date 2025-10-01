# buildSubPlanHash

## Location
[src/backend/executor/nodeSubplan.c:504-674](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeSubplan.c#L504-L674)

## Overview
buildSubPlanHash builds hash tables from subplan output for efficient subquery evaluation, creating separate hash tables for non-null and partially-null rows to support proper SQL NULL semantics.

## Definition
```c
static void buildSubPlanHash(SubPlanState *node, ExprContext *econtext)
```

## Detailed Description
buildSubPlanHash constructs in-memory hash tables by scanning the subplan's output and storing tuples for efficient lookup during subquery evaluation. This function is specifically designed for ANY_SUBLINK operations and implements a sophisticated NULL handling strategy.

The function creates up to two separate hash tables:
1. **Main hash table (hashtable)**: Stores tuples with no NULL values for exact matching
2. **Null hash table (hashnulls)**: Stores tuples containing NULL values for partial matching when unknownEqFalse is false

Key operations include:
- Calculating optimal bucket counts based on estimated plan rows
- Resetting existing hash tables or creating new ones
- Scanning the subplan and projecting tuples into the appropriate hash table
- Managing memory contexts to prevent leaks
- Handling parameter passing between subplan output and hash table storage

The separation of null and non-null tuples optimizes performance by allowing exact hash lookups for the common case (no nulls) while still supporting correct three-valued logic for NULL comparisons.

## Parameters / Member Variables
- `node`: SubPlanState containing hash table structures, projection info, and execution state
- `econtext`: ExprContext providing the evaluation context and memory management

## Dependencies
- Functions called/Symbols referenced:
  - [MemoryContextReset](../M/MemoryContextReset.md) (resets hash table memory context)
  - [clamp_cardinality_to_long](../c/clamp_cardinality_to_long.md) (converts plan rows estimate to bucket count)
  - [BuildTupleHashTableExt](../B/BuildTupleHashTableExt.md) (creates new hash tables with specified parameters)
  - [ResetTupleHashTable](../R/ResetTupleHashTable.md) (clears existing hash table contents)
  - [ExecReScan](../E/ExecReScan.md) (resets subplan to beginning)
  - [ExecProcNode](../E/ExecProcNode.md) (fetches tuples from subplan)
  - [ExecProject](../E/ExecProject.md) (projects subplan output into hash table format)
  - [LookupTupleHashEntry](../L/LookupTupleHashEntry.md) (inserts tuples into hash table)
  - [slot_getattr](../s/slot_getattr.md) (extracts values from tuple slots)
  - [slotNoNulls](../s/slotNoNulls.md) (checks if tuple contains any nulls)
  - ResetExprContext (cleans up expression evaluation context)
  - [ExecClearTuple](../E/ExecClearTuple.md) (clears tuple slot to prevent double-free)
- Called from (representative examples):
  - [ExecHashSubPlan](../E/ExecHashSubPlan.md) (in nodeSubplan.c:118)

## Notes and Other Information
- Only supports ANY_SUBLINK subplan types (enforced by assertion)
- Creates smaller hash table for nulls (1/16 the size of main table, minimum 1 bucket)
- For single-column subplans with nulls, uses only 1 bucket since there can be only one distinct null entry
- Switches to per-query memory context during subplan scanning for proper memory management  
- Handles duplicate elimination automatically through hash table insertion
- Sets havehashrows/havenullrows flags to indicate which tables contain data
- Clears projection slot to prevent double-free issues when subplan context is reset
- The unknownEqFalse flag determines whether null-containing tuples need to be stored
- Uses innerecontext for parameter value extraction to isolate memory usage per tuple

## Simplified Source

```c
static void
buildSubPlanHash(SubPlanState *node, ExprContext *econtext)
{
    SubPlan *subplan = node->subplan;
    PlanState *planstate = node->planstate;
    int ncols = node->numCols;
    MemoryContext oldcontext;
    long nbuckets;
    TupleTableSlot *slot;

    Assert(subplan->subLinkType == ANY_SUBLINK);

    // Reset hash table memory and initialize state
    MemoryContextReset(node->hashtablecxt);
    node->havehashrows = false;
    node->havenullrows = false;

    // Calculate bucket count from estimated rows
    nbuckets = clamp_cardinality_to_long(planstate->plan->plan_rows);
    if (nbuckets < 1) nbuckets = 1;

    // Create or reset main hash table for non-null tuples
    if (node->hashtable)
        ResetTupleHashTable(node->hashtable);
    else
        node->hashtable = BuildTupleHashTableExt(node->parent, node->descRight, ncols,
                                               node->keyColIdx, node->tab_eq_funcoids,
                                               node->tab_hash_funcs, node->tab_collations,
                                               nbuckets, 0, node->planstate->state->es_query_cxt,
                                               node->hashtablecxt, node->hashtempcxt, false);

    // Create separate hash table for null-containing tuples if needed
    if (!subplan->unknownEqFalse) {
        if (ncols == 1)
            nbuckets = 1;  // Only one possible null entry
        else {
            nbuckets /= 16;  // Smaller table for nulls
            if (nbuckets < 1) nbuckets = 1;
        }

        if (node->hashnulls)
            ResetTupleHashTable(node->hashnulls);
        else
            node->hashnulls = BuildTupleHashTableExt(node->parent, node->descRight, ncols,
                                                   node->keyColIdx, node->tab_eq_funcoids,
                                                   node->tab_hash_funcs, node->tab_collations,
                                                   nbuckets, 0, node->planstate->state->es_query_cxt,
                                                   node->hashtablecxt, node->hashtempcxt, false);
    } else {
        node->hashnulls = NULL;
    }

    // Switch to per-query context for subplan execution
    oldcontext = MemoryContextSwitchTo(econtext->ecxt_per_query_memory);

    // Scan subplan and populate hash tables
    ExecReScan(planstate);
    for (slot = ExecProcNode(planstate); !TupIsNull(slot); slot = ExecProcNode(planstate)) {
        int col = 1;
        ListCell *plst;
        bool isnew;

        // Load parameters from subplan output
        foreach(plst, subplan->paramIds) {
            int paramid = lfirst_int(plst);
            ParamExecData *prmdata = &(innerecontext->ecxt_param_exec_vals[paramid]);
            Assert(prmdata->execPlan == NULL);
            prmdata->value = slot_getattr(slot, col, &(prmdata->isnull));
            col++;
        }
        slot = ExecProject(node->projRight);

        // Store in appropriate hash table based on null content
        if (slotNoNulls(slot)) {
            LookupTupleHashEntry(node->hashtable, slot, &isnew, NULL);
            node->havehashrows = true;
        } else if (node->hashnulls) {
            LookupTupleHashEntry(node->hashnulls, slot, &isnew, NULL);
            node->havenullrows = true;
        }

        ResetExprContext(innerecontext);
    }

    // Clean up projection slot to prevent double-free
    ExecClearTuple(node->projRight->pi_state.resultslot);
    MemoryContextSwitchTo(oldcontext);
}
```