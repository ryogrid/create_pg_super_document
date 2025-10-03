# ATRewriteTable

## Location
[src/backend/commands/tablecmds.c:5988-6363](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/tablecmds.c#L5988-L6363)

## Overview
ATRewriteTable performs the actual tuple-by-tuple processing during ALTER TABLE operations, either rewriting data to a new table or validating constraints on the existing table.

## Definition

```c
static void
ATRewriteTable(AlteredTableInfo *tab, Oid OIDNewHeap, LOCKMODE lockmode)
```
## Detailed Description
ATRewriteTable is the core data processing engine for ALTER TABLE operations, responsible for scanning through all tuples in a table and either copying them to a new table with transformations or validating them against new constraints. The function operates in two primary modes: rewrite mode (when OIDNewHeap is valid) where tuples are transformed and copied to a new table, and validation mode (when OIDNewHeap is InvalidOid) where existing tuples are checked against new constraints without physical copying.

The function implements sophisticated tuple processing logic, handling column transformations through expression evaluation, constraint validation through prepared expression states, and proper handling of generated columns, dropped columns, and NOT NULL constraints. It manages memory efficiently by using per-tuple memory contexts and bulk insert states for optimal performance during large table rewrites.

The function processes each tuple by first extracting data from the old tuple, applying any column transformations specified in tab->newvals, evaluating generated column expressions, and then validating all constraints including CHECK constraints, NOT NULL constraints, and partition constraints. For rewrite operations, the transformed tuple is inserted into the new table using optimized bulk insert methods.

## Parameters / Member Variables
- `*tab`: Pointer to AlteredTableInfo containing all transformation and constraint information for the table being processed
- `OIDNewHeap`: OID of the new table for rewrite operations, or InvalidOid for validation-only operations
- `lockmode`: Lock mode to acquire on the new table during rewrite operations
## Dependencies
- Functions called/Symbols referenced:
  - [table_open](../t/table_open.md)
  - [table_close](../t/table_close.md)
  - RelationGetDescr
  - [GetCurrentCommandId](../G/GetCurrentCommandId.md)
  - [GetBulkInsertState](../G/GetBulkInsertState.md)
  - [FreeBulkInsertState](../F/FreeBulkInsertState.md)
  - [CreateExecutorState](../C/CreateExecutorState.md)
  - [FreeExecutorState](../F/FreeExecutorState.md)
  - [ExecPrepareExpr](../E/ExecPrepareExpr.md)
  - [ExecInitExpr](../E/ExecInitExpr.md)
  - GetPerTupleExprContext
  - [MakeSingleTupleTableSlot](../M/MakeSingleTupleTableSlot.md)
  - [ExecDropSingleTupleTableSlot](../E/ExecDropSingleTupleTableSlot.md)
  - [table_slot_callbacks](../t/table_slot_callbacks.md)
  - [ExecStoreAllNullTuple](../E/ExecStoreAllNullTuple.md)
  - [ExecClearTuple](../E/ExecClearTuple.md)
  - [ExecStoreVirtualTuple](../E/ExecStoreVirtualTuple.md)
  - [lappend_int](../l/lappend_int.md)
  - TupleDescAttr
  - [RegisterSnapshot](../R/RegisterSnapshot.md)
  - [UnregisterSnapshot](../U/UnregisterSnapshot.md)
  - [GetLatestSnapshot](../G/GetLatestSnapshot.md)
  - [table_beginscan](../t/table_beginscan.md)
  - [table_endscan](../t/table_endscan.md)
  - [table_scan_getnextslot](../t/table_scan_getnextslot.md)
  - GetPerTupleMemoryContext
  - [MemoryContextSwitchTo](../M/MemoryContextSwitchTo.md)
  - [slot_getallattrs](../s/slot_getallattrs.md)
  - [slot_attisnull](../s/slot_attisnull.md)
  - [ExecEvalExpr](../E/ExecEvalExpr.md)
  - [ExecCheck](../E/ExecCheck.md)
  - ResetExprContext
  - [table_tuple_insert](../t/table_tuple_insert.md)
  - [table_finish_bulk_insert](../t/table_finish_bulk_insert.md)
  - [TransferPredicateLocksToHeapRelation](../T/TransferPredicateLocksToHeapRelation.md)
  - RelationGetRelationName
  - RelationGetRelid
  - CHECK_FOR_INTERRUPTS
- Called from:
  - [ATRewriteTables](ATRewriteTables.md)

## Notes and Other Information
- This function is static and only used within the tablecmds.c module
- Implements two distinct operation modes: rewrite (with new table) and validation (existing table only)
- Uses bulk insert optimization with TABLE_INSERT_SKIP_FSM for better performance during rewrites
- Handles complex tuple transformation including column defaults, generated columns, and dropped columns
- Implements comprehensive constraint validation including CHECK, NOT NULL, and partition constraints
- Uses per-tuple memory contexts to prevent memory leaks during large table scans
- Transfers predicate locks to relation level before rewriting to maintain serializable isolation
- Supports interruption via CHECK_FOR_INTERRUPTS for long-running operations
- Provides detailed error reporting with table and column context for constraint violations
- Processes generated columns in two phases: first non-generated, then generated columns
- Located at src/backend/commands/tablecmds.c:5988-6363

## Simplified Source

```c
static void
ATRewriteTable(AlteredTableInfo *tab, Oid OIDNewHeap, LOCKMODE lockmode)
{
    Relation oldrel, newrel;
    TupleDesc oldTupDesc, newTupDesc;
    bool needscan = false;
    List *notnull_attrs;
    EState *estate;
    CommandId mycid;
    BulkInsertState bistate;

    // Open old table and possibly new table
    oldrel = table_open(tab->relid, NoLock);
    oldTupDesc = tab->oldDesc;
    newTupDesc = RelationGetDescr(oldrel);

    if (OidIsValid(OIDNewHeap))
        newrel = table_open(OIDNewHeap, lockmode);
    else
        newrel = NULL;

    // Set up bulk insert state for new table
    if (newrel)
    {
        mycid = GetCurrentCommandId(true);
        bistate = GetBulkInsertState();
    }

    // Create executor state for expression evaluation
    estate = CreateExecutorState();

    // Prepare constraint expressions
    foreach(l, tab->constraints)
    {
        NewConstraint *con = lfirst(l);
        if (con->contype == CONSTR_CHECK)
        {
            needscan = true;
            con->qualstate = ExecPrepareExpr((Expr *) con->qual, estate);
        }
    }

    // Prepare partition constraint if present
    if (tab->partition_constraint)
    {
        needscan = true;
        partqualstate = ExecPrepareExpr(tab->partition_constraint, estate);
    }

    // Prepare column transformation expressions
    foreach(l, tab->newvals)
    {
        NewColumnValue *ex = lfirst(l);
        ex->exprstate = ExecInitExpr((Expr *) ex->expr, NULL);
    }

    // Build list of NOT NULL columns to check
    if (newrel || tab->verify_new_notnull)
    {
        for (i = 0; i < newTupDesc->natts; i++)
        {
            Form_pg_attribute attr = TupleDescAttr(newTupDesc, i);
            if (attr->attnotnull && !attr->attisdropped)
                notnull_attrs = lappend_int(notnull_attrs, i);
        }
        if (notnull_attrs)
            needscan = true;
    }

    // Main tuple processing loop
    if (newrel || needscan)
    {
        ExprContext *econtext;
        TupleTableSlot *oldslot, *newslot;
        TableScanDesc scan;
        Snapshot snapshot;

        // Set up tuple slots and scan
        if (tab->rewrite)
        {
            oldslot = MakeSingleTupleTableSlot(oldTupDesc, table_slot_callbacks(oldrel));
            newslot = MakeSingleTupleTableSlot(newTupDesc, table_slot_callbacks(newrel));
            ExecStoreAllNullTuple(newslot);
        }
        else
        {
            oldslot = MakeSingleTupleTableSlot(newTupDesc, table_slot_callbacks(oldrel));
            newslot = NULL;
        }

        econtext = GetPerTupleExprContext(estate);
        snapshot = RegisterSnapshot(GetLatestSnapshot());
        scan = table_beginscan(oldrel, snapshot, 0, NULL);

        // Process each tuple
        while (table_scan_getnextslot(scan, ForwardScanDirection, oldslot))
        {
            TupleTableSlot *insertslot;

            if (tab->rewrite > 0)
            {
                // Copy old tuple data to new slot
                slot_getallattrs(oldslot);
                ExecClearTuple(newslot);
                memcpy(newslot->tts_values, oldslot->tts_values,
                       sizeof(Datum) * oldslot->tts_nvalid);
                memcpy(newslot->tts_isnull, oldslot->tts_isnull,
                       sizeof(bool) * oldslot->tts_nvalid);

                // Apply column transformations
                econtext->ecxt_scantuple = oldslot;
                foreach(l, tab->newvals)
                {
                    NewColumnValue *ex = lfirst(l);
                    if (!ex->is_generated)
                    {
                        newslot->tts_values[ex->attnum - 1] =
                            ExecEvalExpr(ex->exprstate, econtext,
                                         &newslot->tts_isnull[ex->attnum - 1]);
                    }
                }

                ExecStoreVirtualTuple(newslot);

                // Evaluate generated columns
                econtext->ecxt_scantuple = newslot;
                foreach(l, tab->newvals)
                {
                    NewColumnValue *ex = lfirst(l);
                    if (ex->is_generated)
                    {
                        newslot->tts_values[ex->attnum - 1] =
                            ExecEvalExpr(ex->exprstate, econtext,
                                         &newslot->tts_isnull[ex->attnum - 1]);
                    }
                }

                insertslot = newslot;
            }
            else
            {
                insertslot = oldslot;
            }

            // Validate constraints
            econtext->ecxt_scantuple = insertslot;

            // Check NOT NULL constraints
            foreach(l, notnull_attrs)
            {
                int attn = lfirst_int(l);
                if (slot_attisnull(insertslot, attn + 1))
                    ereport(ERROR, (errcode(ERRCODE_NOT_NULL_VIOLATION),
                                    errmsg("column contains null values")));
            }

            // Check CHECK constraints
            foreach(l, tab->constraints)
            {
                NewConstraint *con = lfirst(l);
                if (con->contype == CONSTR_CHECK)
                {
                    if (!ExecCheck(con->qualstate, econtext))
                        ereport(ERROR, (errcode(ERRCODE_CHECK_VIOLATION),
                                        errmsg("check constraint violated")));
                }
            }

            // Check partition constraint
            if (partqualstate && !ExecCheck(partqualstate, econtext))
                ereport(ERROR, (errcode(ERRCODE_CHECK_VIOLATION),
                                errmsg("partition constraint violated")));

            // Insert tuple into new table if rewriting
            if (newrel)
                table_tuple_insert(newrel, insertslot, mycid,
                                   TABLE_INSERT_SKIP_FSM, bistate);

            ResetExprContext(econtext);
            CHECK_FOR_INTERRUPTS();
        }

        // Clean up scan resources
        table_endscan(scan);
        UnregisterSnapshot(snapshot);
        ExecDropSingleTupleTableSlot(oldslot);
        if (newslot)
            ExecDropSingleTupleTableSlot(newslot);
    }

    // Clean up
    FreeExecutorState(estate);
    table_close(oldrel, NoLock);
    if (newrel)
    {
        FreeBulkInsertState(bistate);
        table_finish_bulk_insert(newrel, TABLE_INSERT_SKIP_FSM);
        table_close(newrel, NoLock);
    }
}
```