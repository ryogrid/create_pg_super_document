# ATRewriteTable

## Location
src/backend/commands/tablecmds.c: 5988 - 6363

## Overview
ATRewriteTable performs the actual tuple-by-tuple processing during ALTER TABLE operations, either rewriting data to a new table or validating constraints on the existing table.

## Definition


## Detailed Description
ATRewriteTable is the core data processing engine for ALTER TABLE operations, responsible for scanning through all tuples in a table and either copying them to a new table with transformations or validating them against new constraints. The function operates in two primary modes: rewrite mode (when OIDNewHeap is valid) where tuples are transformed and copied to a new table, and validation mode (when OIDNewHeap is InvalidOid) where existing tuples are checked against new constraints without physical copying.

The function implements sophisticated tuple processing logic, handling column transformations through expression evaluation, constraint validation through prepared expression states, and proper handling of generated columns, dropped columns, and NOT NULL constraints. It manages memory efficiently by using per-tuple memory contexts and bulk insert states for optimal performance during large table rewrites.

The function processes each tuple by first extracting data from the old tuple, applying any column transformations specified in tab->newvals, evaluating generated column expressions, and then validating all constraints including CHECK constraints, NOT NULL constraints, and partition constraints. For rewrite operations, the transformed tuple is inserted into the new table using optimized bulk insert methods.

## Parameters / Member Variables
- : Pointer to AlteredTableInfo containing all transformation and constraint information for the table being processed
- : OID of the new table for rewrite operations, or InvalidOid for validation-only operations
- : Lock mode to acquire on the new table during rewrite operations

## Dependencies
- Functions called/Symbols referenced:
  - table_open
  - table_close
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
  - ExecClearTuple
  - [ExecStoreVirtualTuple](../E/ExecStoreVirtualTuple.md)
  - lappend_int
  - TupleDescAttr
  - RegisterSnapshot
  - UnregisterSnapshot
  - GetLatestSnapshot
  - [table_beginscan](../t/table_beginscan.md)
  - [table_endscan](../t/table_endscan.md)
  - [table_scan_getnextslot](../t/table_scan_getnextslot.md)
  - GetPerTupleMemoryContext
  - [MemoryContextSwitchTo](../M/MemoryContextSwitchTo.md)
  - slot_getallattrs
  - slot_attisnull
  - ExecEvalExpr
  - [ExecCheck](../E/ExecCheck.md)
  - ResetExprContext
  - table_tuple_insert
  - table_finish_bulk_insert
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