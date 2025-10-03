# compute_index_stats

## Location
[src/backend/commands/analyze.c:828-998](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/analyze.c#L828-L998)

## Overview
Computes statistics for index expressions and partial index predicates by evaluating them against sampled table rows.

## Definition

```c
static void
compute_index_stats(Relation onerel, double totalrows,
					AnlIndexData *indexdata, int nindexes,
					HeapTuple *rows, int numrows,
					MemoryContext col_context)
```
## Detailed Description
This function processes index expressions and partial index predicates to generate statistics for the query planner. For each index, it sets up an execution environment to evaluate expressions and predicates against the sampled rows. It handles partial indexes by checking predicate conditions to determine which rows would actually be included in the index. For expression indexes, it evaluates the expressions to extract values for statistical analysis.

The function creates a separate memory context for index processing, sets up executor state and expression contexts for evaluation, and processes each sampled row to evaluate predicates and expressions. It calculates the fraction of rows that satisfy partial index predicates and uses this to estimate the total index size. Finally, it computes statistics for expression columns using the extracted values.

## Parameters / Member Variables
- `onerel`: The table relation being analyzed
- `totalrows`: Total estimated number of rows in the table
- `*indexdata`: Array of AnlIndexData structures containing index information and statistics
- `nindexes`: Number of indexes to process
- `*rows`: Array of sampled HeapTuple rows from the table
- `numrows`: Number of rows in the sample
- `col_context`: Memory context for temporary column statistics computation
## Dependencies
- Functions called/Symbols referenced:
  - /: Executor state management for expression evaluation
  - : Gets per-tuple expression context for evaluation
  - : Prepares predicate for execution
  - : Evaluates index expressions to produce datum values
  - : Evaluates partial index predicates
  - : Copies datum values with proper memory management
  - : Index-specific fetch function for statistics computation
- Called from (representative examples):
  - : Main analysis function when processing indexes

## Notes and Other Information
- Creates a dedicated 'Analyze Index' memory context for index processing
- Skips indexes with no analyzable columns and no partial predicate
- Uses executor state and expression contexts to safely evaluate complex expressions
- Handles both partial indexes (with predicates) and expression indexes
- Calculates  to estimate what fraction of table rows appear in each index
- Processes expression values in strided format for efficient statistics computation
- Properly manages memory contexts to prevent leaks during expression evaluation
- Resets expression context for each row to reclaim temporary memory used during evaluation

## Simplified Source

```c
static void compute_index_stats(Relation onerel, double totalrows,
                               AnlIndexData *indexdata, int nindexes,
                               HeapTuple *rows, int numrows,
                               MemoryContext col_context) {
    MemoryContext ind_context, old_context;
    Datum values[INDEX_MAX_KEYS];
    bool isnull[INDEX_MAX_KEYS];

    // Create dedicated memory context for index processing
    ind_context = AllocSetContextCreate(anl_context, "Analyze Index", ALLOCSET_DEFAULT_SIZES);
    old_context = MemoryContextSwitchTo(ind_context);

    // Process each index
    for (int ind = 0; ind < nindexes; ind++) {
        AnlIndexData *thisdata = &indexdata[ind];
        IndexInfo *indexInfo = thisdata->indexInfo;
        int attr_cnt = thisdata->attr_cnt;

        // Skip indexes with no analyzable content
        if (attr_cnt == 0 && indexInfo->ii_Predicate == NIL)
            continue;

        // Set up execution environment for expression evaluation
        EState *estate = CreateExecutorState();
        ExprContext *econtext = GetPerTupleExprContext(estate);
        TupleTableSlot *slot = MakeSingleTupleTableSlot(RelationGetDescr(onerel), &TTSOpsHeapTuple);
        econtext->ecxt_scantuple = slot;

        // Prepare partial index predicate if it exists
        ExprState *predicate = ExecPrepareQual(indexInfo->ii_Predicate, estate);

        // Allocate arrays for expression values
        Datum *exprvals = palloc(numrows * attr_cnt * sizeof(Datum));
        bool *exprnulls = palloc(numrows * attr_cnt * sizeof(bool));

        int numindexrows = 0, tcnt = 0;

        // Process each sampled row
        for (int rowno = 0; rowno < numrows; rowno++) {
            HeapTuple heapTuple = rows[rowno];

            vacuum_delay_point();
            ResetExprContext(econtext);
            ExecStoreHeapTuple(heapTuple, slot, false);

            // Check partial index predicate if present
            if (predicate != NULL) {
                if (!ExecQual(predicate, econtext))
                    continue;  // Row doesn't satisfy predicate
            }
            numindexrows++;

            // Evaluate expressions if this index has analyzable columns
            if (attr_cnt > 0) {
                // Compute index expressions for this row
                FormIndexDatum(indexInfo, slot, estate, values, isnull);

                // Extract and store values for statistics
                for (int i = 0; i < attr_cnt; i++) {
                    VacAttrStats *stats = thisdata->vacattrstats[i];
                    int attnum = stats->tupattnum;

                    if (isnull[attnum - 1]) {
                        exprvals[tcnt] = (Datum) 0;
                        exprnulls[tcnt] = true;
                    } else {
                        exprvals[tcnt] = datumCopy(values[attnum - 1],
                                                  stats->attrtype->typbyval,
                                                  stats->attrtype->typlen);
                        exprnulls[tcnt] = false;
                    }
                    tcnt++;
                }
            }
        }

        // Calculate fraction of rows that pass predicate
        thisdata->tupleFract = (double) numindexrows / (double) numrows;
        double totalindexrows = ceil(thisdata->tupleFract * totalrows);

        // Compute statistics for expression columns
        if (numindexrows > 0) {
            MemoryContextSwitchTo(col_context);
            for (int i = 0; i < attr_cnt; i++) {
                VacAttrStats *stats = thisdata->vacattrstats[i];

                // Set up statistics computation parameters
                stats->exprvals = exprvals + i;
                stats->exprnulls = exprnulls + i;
                stats->rowstride = attr_cnt;

                // Compute the actual statistics
                stats->compute_stats(stats, ind_fetch_func, numindexrows, totalindexrows);
                MemoryContextReset(col_context);
            }
        }

        // Clean up this index's execution environment
        MemoryContextSwitchTo(ind_context);
        ExecDropSingleTupleTableSlot(slot);
        FreeExecutorState(estate);
        MemoryContextReset(ind_context);
    }

    // Restore memory context and clean up
    MemoryContextSwitchTo(old_context);
    MemoryContextDelete(ind_context);
}
```