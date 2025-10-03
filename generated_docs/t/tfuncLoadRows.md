# tfuncLoadRows

## Location
[src/backend/executor/nodeTableFuncscan.c:435-524](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeTableFuncscan.c#L435-L524)

## Overview
This static function loads all rows from a TableFunc table builder into a tuplestore by iterating through each row and column to populate tuple values with proper handling of ordinality columns, default expressions, and NOT NULL constraints.

## Definition
```c
static void tfuncLoadRows(TableFuncScanState *tstate, ExprContext *econtext)
```

## Detailed Description
tfuncLoadRows is responsible for the actual data extraction phase of table function execution. It continuously fetches rows from the table builder until no more rows are available, processing each column value according to its type and constraints. The function handles ordinality columns by automatically incrementing a counter, retrieves regular column values through the routine's GetValue method, and applies default expressions when values are null. It also enforces NOT NULL constraints and manages memory efficiently by using per-tuple context that gets reset after each row. All processed tuples are stored in the tuplestore for later retrieval.

## Parameters / Member Variables
- `tstate`: TableFuncScanState pointer containing the scan state, tuplestore, and table function configuration
- `econtext`: ExprContext pointer providing the evaluation context for default expressions

## Dependencies
- Functions called/Symbols referenced:
  - [list_head](../l/list_head.md)
  - [ExecClearTuple](../E/ExecClearTuple.md)
  - [ExecEvalExpr](../E/ExecEvalExpr.md)
  - [bms_is_member](../b/bms_is_member.md)
  - [lnext](../l/lnext.md)
  - [tuplestore_putvalues](tuplestore_putvalues.md)
  - [MemoryContextReset](../M/MemoryContextReset.md)
- Called from (representative examples):
  - [tfuncFetchRows](tfuncFetchRows.md)

## Notes and Other Information
- Implements the core row processing loop for table functions
- Handles special ordinality column processing with automatic incrementing
- Supports default expressions for columns when values are missing
- Enforces NOT NULL constraints with appropriate error reporting
- Uses per-tuple memory context for efficient memory management
- Integrates with CHECK_FOR_INTERRUPTS() for query cancellation support
- Essential component of XMLTABLE and JSON_TABLE data extraction pipeline

## Simplified Source
```c
static void
tfuncLoadRows(TableFuncScanState *tstate, ExprContext *econtext)
{
    const TableFuncRoutine *routine = tstate->routine;
    TupleTableSlot *slot = tstate->ss.ss_ScanTupleSlot;
    TupleDesc tupdesc = slot->tts_tupleDescriptor;
    Datum *values = slot->tts_values;
    bool *nulls = slot->tts_isnull;
    int natts = tupdesc->natts;
    int ordinalitycol = ((TableFuncScan *) (tstate->ss.ps.plan))->tablefunc->ordinalitycol;

    // Switch to per-tuple memory context for cleanup
    MemoryContext oldcxt = MemoryContextSwitchTo(econtext->ecxt_per_tuple_memory);

    // Process each row from the table builder
    while (routine->FetchRow(tstate))
    {
        ListCell *cell = list_head(tstate->coldefexprs);

        CHECK_FOR_INTERRUPTS();
        ExecClearTuple(slot);

        // Process each column in the row
        for (int colno = 0; colno < natts; colno++)
        {
            Form_pg_attribute att = TupleDescAttr(tupdesc, colno);

            if (colno == ordinalitycol)
            {
                // Handle ordinality column - auto-increment counter
                values[colno] = Int32GetDatum(tstate->ordinal++);
                nulls[colno] = false;
            }
            else
            {
                bool isnull;

                // Get column value from table builder
                values[colno] = routine->GetValue(tstate, colno, att->atttypid,
                                                att->atttypmod, &isnull);

                // Apply default expression if value is null
                if (isnull && cell != NULL)
                {
                    ExprState *coldefexpr = (ExprState *) lfirst(cell);
                    if (coldefexpr != NULL)
                        values[colno] = ExecEvalExpr(coldefexpr, econtext, &isnull);
                }

                // Check NOT NULL constraint
                if (isnull && bms_is_member(colno, tstate->notnulls))
                    ereport(ERROR, (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                            errmsg("null is not allowed in column \"%s\"",
                                   NameStr(att->attname))));

                nulls[colno] = isnull;
            }

            // Move to next default expression
            if (cell != NULL)
                cell = lnext(tstate->coldefexprs, cell);
        }

        // Store the completed tuple
        tuplestore_putvalues(tstate->tupstore, tupdesc, values, nulls);
        MemoryContextReset(econtext->ecxt_per_tuple_memory);
    }

    MemoryContextSwitchTo(oldcxt);
}
```