# compute_expr_stats

## Location
[src/backend/statistics/extended_stats.c:2090-2233](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/statistics/extended_stats.c#L2090-L2233)

## Overview
Computes statistics for expression columns by evaluating expressions against sampled table rows and generating statistical summaries for use in query optimization.

## Definition

```c
static void
compute_expr_stats(Relation onerel, double totalrows,
				   AnlExprData *exprdata, int nexprs,
				   HeapTuple *rows, int numrows)
```
## Detailed Description
This function evaluates expressions on a sample of table rows and computes detailed statistics for each expression. It creates an executor state for expression evaluation, processes each expression against all sample rows, and generates statistical summaries including histograms, most common values, and n_distinct estimates. The function uses proper memory context management to avoid memory leaks during expression evaluation and handles null values appropriately. Statistics computed by this function are essential for the query planner to make accurate cost estimates for queries involving expressions.

## Parameters / Member Variables
- : The relation being analyzed for statistics computation
- : Total number of rows in the relation (used for statistical extrapolation)
- : Array of AnlExprData structures containing expression information and VacAttrStats objects
- : Number of expressions in the exprdata array to process
- : Array of HeapTuple pointers representing the sample rows to evaluate expressions against
- : Number of sample rows in the rows array

## Dependencies
- Functions called/Symbols referenced:
  - [CreateExecutorState](../C/CreateExecutorState.md)
  - GetPerTupleExprContext
  - [ExecPrepareExpr](../E/ExecPrepareExpr.md)
  - [MakeSingleTupleTableSlot](../M/MakeSingleTupleTableSlot.md)
  - [ExecStoreHeapTuple](../E/ExecStoreHeapTuple.md)
  - [ExecEvalExprSwitchContext](../E/ExecEvalExprSwitchContext.md)
  - ResetExprContext
  - [ExecDropSingleTupleTableSlot](../E/ExecDropSingleTupleTableSlot.md)
  - [FreeExecutorState](../F/FreeExecutorState.md)
  - AllocSetContextCreate
  - [datumCopy](../d/datumCopy.md)
  - [get_attribute_options](../g/get_attribute_options.md)
  - [expr_fetch_func](../e/expr_fetch_func.md)
  - [MemoryContextReset](../M/MemoryContextReset.md)
  - [MemoryContextDelete](../M/MemoryContextDelete.md)
- Called from (representative examples):
  - [BuildRelationExtStatistics](../B/BuildRelationExtStatistics.md)

## Notes and Other Information
The function creates a dedicated memory context for expression evaluation to prevent memory leaks and ensure proper cleanup. Each expression is evaluated against all sample rows using PostgreSQL's expression evaluation infrastructure. The computed statistics are stored in the VacAttrStats structure and can be overridden by table-specific n_distinct options. Memory management is critical as expression evaluation can generate significant temporary data, so the function resets the per-tuple context after each row evaluation and cleans up all resources at the end.

## Simplified Source

```c
static void compute_expr_stats(Relation onerel, double totalrows,
                               AnlExprData *exprdata, int nexprs,
                               HeapTuple *rows, int numrows)
{
    MemoryContext expr_context, old_context;
    int ind, i;

    // Create memory context for expression evaluation
    expr_context = AllocSetContextCreate(CurrentMemoryContext,
                                         "Analyze Expression",
                                         ALLOCSET_DEFAULT_SIZES);
    old_context = MemoryContextSwitchTo(expr_context);

    // Process each expression
    for (ind = 0; ind < nexprs; ind++)
    {
        AnlExprData *thisdata = &exprdata[ind];
        VacAttrStats *stats = thisdata->vacattrstat;
        Node *expr = thisdata->expr;
        TupleTableSlot *slot;
        EState *estate;
        ExprContext *econtext;
        Datum *exprvals;
        bool *exprnulls;
        ExprState *exprstate;
        int tcnt;

        // Set up expression evaluation infrastructure
        estate = CreateExecutorState();
        econtext = GetPerTupleExprContext(estate);
        exprstate = ExecPrepareExpr((Expr *) expr, estate);
        slot = MakeSingleTupleTableSlot(RelationGetDescr(onerel), &TTSOpsHeapTuple);
        econtext->ecxt_scantuple = slot;

        // Allocate arrays for expression results
        exprvals = (Datum *) palloc(numrows * sizeof(Datum));
        exprnulls = (bool *) palloc(numrows * sizeof(bool));

        // Evaluate expression against each sample row
        tcnt = 0;
        for (i = 0; i < numrows; i++)
        {
            Datum datum;
            bool isnull;

            // Reset context and store current tuple
            ResetExprContext(econtext);
            ExecStoreHeapTuple(rows[i], slot, false);

            // Evaluate expression and store result
            datum = ExecEvalExprSwitchContext(exprstate, GetPerTupleExprContext(estate), &isnull);
            if (isnull)
            {
                exprvals[tcnt] = (Datum) 0;
                exprnulls[tcnt] = true;
            }
            else
            {
                exprvals[tcnt] = datumCopy(datum, stats->attrtype->typbyval, stats->attrtype->typlen);
                exprnulls[tcnt] = false;
            }
            tcnt++;
        }

        // Compute statistics if we have data
        if (tcnt > 0)
        {
            AttributeOpts *aopt = get_attribute_options(onerel->rd_id, stats->tupattnum);

            stats->exprvals = exprvals;
            stats->exprnulls = exprnulls;
            stats->rowstride = 1;
            stats->compute_stats(stats, expr_fetch_func, tcnt, tcnt);

            // Override with table-specific n_distinct if specified
            if (aopt != NULL && aopt->n_distinct != 0.0)
                stats->stadistinct = aopt->n_distinct;
        }

        // Clean up expression resources
        MemoryContextSwitchTo(expr_context);
        ExecDropSingleTupleTableSlot(slot);
        FreeExecutorState(estate);
        MemoryContextReset(expr_context);
    }

    // Clean up expression context
    MemoryContextSwitchTo(old_context);
    MemoryContextDelete(expr_context);
}
```