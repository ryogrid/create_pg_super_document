# make_build_data

## Location
[src/backend/statistics/extended_stats.c:2452-2617](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/statistics/extended_stats.c#L2452-L2617)

## Overview
Creates and populates a StatsBuildData structure containing evaluated expression values and column data for building extended statistics.

## Definition

```c
static StatsBuildData *
make_build_data(Relation rel, StatExtEntry *stat, int numrows, HeapTuple *rows,
				VacAttrStats **stats, int stattarget)
```
## Detailed Description
This function prepares data needed for building extended statistics by evaluating expressions and extracting column values from a sample of table rows. It creates a comprehensive data structure that holds both regular column values and computed expression results, which are then used to build various types of extended statistics (functional dependencies, N-distinct, MCV lists, etc.).

The function allocates a single memory chunk containing arrays for attribute numbers, statistics metadata, and data values/nulls for both columns and expressions. For regular columns, it extracts values directly from the heap tuples. For expressions, it sets up an executor state and evaluates each expression against every sample row, storing the results in the same format as column data.

The resulting StatsBuildData structure provides a uniform interface for accessing both column and expression data during statistics computation, abstracting away the differences between simple column references and complex expressions.

## Parameters / Member Variables
- : Relation for which statistics are being built
- : StatExtEntry containing information about the extended statistics object (columns, expressions, types)
- : Number of sample rows to process
- : Array of HeapTuple pointers containing the sample data
- : Array of VacAttrStats for the columns being analyzed
- : Statistics target controlling the level of detail in statistics

## Dependencies
- Functions called/Symbols referenced:
  - [bms_num_members](../b/bms_num_members.md), bms_next_member, examine_expression, heap_getattr
  - [CreateExecutorState](../C/CreateExecutorState.md), GetPerTupleExprContext, MakeSingleTupleTableSlot
  - [ExecPrepareExprList](../E/ExecPrepareExprList.md), ResetExprContext, ExecStoreHeapTuple, ExecEvalExpr
  - [ExecDropSingleTupleTableSlot](../E/ExecDropSingleTupleTableSlot.md), FreeExecutorState
- Called from (representative examples):
  - [BuildRelationExtStatistics](../B/BuildRelationExtStatistics.md)

## Notes and Other Information
- Allocates all memory in a single chunk for efficient cleanup
- Uses PostgreSQL's expression evaluation infrastructure for computing expression values
- Handles memory management carefully to avoid leaks during expression evaluation
- The resulting data structure is used by various extended statistics building functions
- Critical for extended statistics that involve expressions, not just simple column combinations
- Expression evaluation is performed in a per-tuple context that is reset for each row to prevent memory accumulation

## Simplified Source

```c
static StatsBuildData *make_build_data(Relation rel, StatExtEntry *stat, int numrows, HeapTuple *rows,
                                       VacAttrStats **stats, int stattarget)
{
    StatsBuildData *result;
    char *ptr;
    Size len;
    int i, k, idx;
    TupleTableSlot *slot;
    EState *estate;
    ExprContext *econtext;
    List *exprstates = NIL;
    int nkeys = bms_num_members(stat->columns) + list_length(stat->exprs);
    ListCell *lc;

    // Calculate memory needed for all data structures
    len = MAXALIGN(sizeof(StatsBuildData));
    len += MAXALIGN(sizeof(AttrNumber) * nkeys);     // attnums
    len += MAXALIGN(sizeof(VacAttrStats *) * nkeys); // stats
    len += MAXALIGN(sizeof(Datum *) * nkeys);        // values pointer array
    len += nkeys * MAXALIGN(sizeof(Datum) * numrows); // actual values
    len += MAXALIGN(sizeof(bool *) * nkeys);         // nulls pointer array
    len += nkeys * MAXALIGN(sizeof(bool) * numrows); // actual nulls

    // Allocate single chunk and set up pointers
    ptr = palloc(len);
    result = (StatsBuildData *) ptr;
    ptr += MAXALIGN(sizeof(StatsBuildData));

    result->attnums = (AttrNumber *) ptr;
    ptr += MAXALIGN(sizeof(AttrNumber) * nkeys);

    result->stats = (VacAttrStats **) ptr;
    ptr += MAXALIGN(sizeof(VacAttrStats *) * nkeys);

    result->values = (Datum **) ptr;
    ptr += MAXALIGN(sizeof(Datum *) * nkeys);

    result->nulls = (bool **) ptr;
    ptr += MAXALIGN(sizeof(bool *) * nkeys);

    // Set up individual value/null arrays
    for (i = 0; i < nkeys; i++)
    {
        result->values[i] = (Datum *) ptr;
        ptr += MAXALIGN(sizeof(Datum) * numrows);

        result->nulls[i] = (bool *) ptr;
        ptr += MAXALIGN(sizeof(bool) * numrows);
    }

    // Initialize basic structure info
    result->nattnums = nkeys;
    result->numrows = numrows;

    // Fill attribute info - first columns, then expressions
    idx = 0;
    k = -1;
    while ((k = bms_next_member(stat->columns, k)) >= 0)
    {
        result->attnums[idx] = k;
        result->stats[idx] = stats[idx];
        idx++;
    }

    k = -1;
    foreach(lc, stat->exprs)
    {
        Node *expr = (Node *) lfirst(lc);
        result->attnums[idx] = k;
        result->stats[idx] = examine_expression(expr, stattarget);
        idx++;
        k--;
    }

    // Extract values for regular columns
    for (i = 0; i < numrows; i++)
    {
        idx = 0;
        k = -1;
        while ((k = bms_next_member(stat->columns, k)) >= 0)
        {
            result->values[idx][i] = heap_getattr(rows[i], k,
                                                  result->stats[idx]->tupDesc,
                                                  &result->nulls[idx][i]);
            idx++;
        }
    }

    // Set up expression evaluation infrastructure
    estate = CreateExecutorState();
    econtext = GetPerTupleExprContext(estate);
    slot = MakeSingleTupleTableSlot(RelationGetDescr(rel), &TTSOpsHeapTuple);
    econtext->ecxt_scantuple = slot;
    exprstates = ExecPrepareExprList(stat->exprs, estate);

    // Evaluate expressions for each row
    for (i = 0; i < numrows; i++)
    {
        ResetExprContext(econtext);
        ExecStoreHeapTuple(rows[i], slot, false);

        idx = bms_num_members(stat->columns);
        foreach(lc, exprstates)
        {
            Datum datum;
            bool isnull;
            ExprState *exprstate = (ExprState *) lfirst(lc);

            datum = ExecEvalExpr(exprstate, GetPerTupleExprContext(estate), &isnull);
            if (isnull)
            {
                result->values[idx][i] = (Datum) 0;
                result->nulls[idx][i] = true;
            }
            else
            {
                result->values[idx][i] = datum;
                result->nulls[idx][i] = false;
            }

            idx++;
        }
    }

    // Clean up expression evaluation resources
    ExecDropSingleTupleTableSlot(slot);
    FreeExecutorState(estate);

    return result;
}
```