# tfuncFetchRows

## Location
[src/backend/executor/nodeTableFuncscan.c:268-339](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeTableFuncscan.c#L268-L339)

## Overview
This static function reads rows from a TableFunc producer by initializing the table function, evaluating the document expression, and loading all resulting rows into a tuplestore.

## Definition
```c
static void tfuncFetchRows(TableFuncScanState *tstate, ExprContext *econtext)
```

## Detailed Description
tfuncFetchRows is responsible for fetching all rows from a table function (such as XMLTABLE or JSON_TABLE) and storing them in a tuplestore for subsequent retrieval. The function operates in multiple phases: first it creates a tuplestore in per-query memory, then switches to per-table context for the actual data processing. It evaluates the document expression and if the result is not NULL, initializes the table function and loads all rows. The function includes proper exception handling to ensure cleanup of opaque state if errors occur during processing.

## Parameters / Member Variables
- `tstate`: TableFuncScanState pointer containing the scan state and configuration
- `econtext`: ExprContext pointer for expression evaluation context

## Dependencies
- Functions called/Symbols referenced:
  - [tuplestore_begin_heap](tuplestore_begin_heap.md)
  - [ExecEvalExpr](../E/ExecEvalExpr.md)
  - [tfuncInitialize](tfuncInitialize.md)
  - [tfuncLoadRows](tfuncLoadRows.md)
  - PG_TRY/PG_CATCH/PG_RE_THROW/PG_END_TRY
  - [MemoryContextReset](../M/MemoryContextReset.md)
- Called from (representative examples):
  - [TableFuncNext](../T/TableFuncNext.md)

## Notes and Other Information
- Uses per-table memory context to manage potentially large memory allocations
- Implements proper exception handling with PG_TRY/PG_CATCH blocks
- Handles NULL document expressions by returning empty results
- Essential for XMLTABLE and JSON_TABLE functionality in lateral joins
- Manages opaque state lifecycle including cleanup on errors
- Initializes ordinality counter for row numbering

## Simplified Source

```c
static void
tfuncFetchRows(TableFuncScanState *tstate, ExprContext *econtext)
{
    const TableFuncRoutine *routine = tstate->routine;
    MemoryContext oldcxt;
    Datum value;
    bool isnull;

    Assert(tstate->opaque == NULL);

    // Create tuplestore for results in per-query memory
    oldcxt = MemoryContextSwitchTo(econtext->ecxt_per_query_memory);
    tstate->tupstore = tuplestore_begin_heap(false, false, work_mem);

    // Switch to per-table context for data processing
    MemoryContextSwitchTo(tstate->perTableCxt);

    PG_TRY();
    {
        // Initialize the table function routine
        routine->InitOpaque(tstate,
                           tstate->ss.ss_ScanTupleSlot->tts_tupleDescriptor->natts);

        // Evaluate the document expression
        value = ExecEvalExpr(tstate->docexpr, econtext, &isnull);

        if (!isnull)
        {
            // Initialize table function with document value
            tfuncInitialize(tstate, econtext, value);

            // Start ordinality counter
            tstate->ordinal = 1;

            // Load all rows into tuplestore
            tfuncLoadRows(tstate, econtext);
        }
    }
    PG_CATCH();
    {
        // Cleanup on error
        if (tstate->opaque != NULL)
            routine->DestroyOpaque(tstate);
        PG_RE_THROW();
    }
    PG_END_TRY();

    // Final cleanup
    if (tstate->opaque != NULL)
    {
        routine->DestroyOpaque(tstate);
        tstate->opaque = NULL;
    }

    // Restore original memory context and reset per-table context
    MemoryContextSwitchTo(oldcxt);
    MemoryContextReset(tstate->perTableCxt);
}
```