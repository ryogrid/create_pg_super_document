# execCurrentOf

## Location
[src/backend/executor/execCurrent.c:44-257](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execCurrent.c#L44-L257)

## Overview
Determines which row of a specified table is currently being scanned by a named cursor in a CURRENT OF expression, returning the row's TID (tuple identifier).

## Definition

```c
bool
execCurrentOf(CurrentOfExpr *cexpr,
			  ExprContext *econtext,
			  Oid table_oid,
			  ItemPointer current_tid)
```
## Detailed Description
The execCurrentOf function implements the core logic for PostgreSQL's "WHERE CURRENT OF cursor" functionality, which allows UPDATE and DELETE statements to target the row currently positioned by a cursor. The function uses two different strategies depending on whether the cursor query uses FOR UPDATE/SHARE row locking or not:

1. **FOR UPDATE/SHARE strategy**: Extracts the current tuple identifier (TID) directly from the ExecRowMark structure, which tracks row locks. This approach can identify target tables in many cases where the non-FOR-UPDATE approach cannot.

2. **Non-FOR-UPDATE strategy**: Searches through the cursor's execution plan tree to find the scan node for the specified table, then extracts the TID from the scan's current tuple. This approach allows WHERE CURRENT OF to work with insensitive cursors.

The function validates that the cursor exists, is a SELECT query, is not a held cursor from a previous transaction, and is currently positioned on a row. It handles inheritance cases where multiple tables might be involved by returning false (rather than an error) when the specified table didn't produce the cursor's current row.

## Parameters / Member Variables
- `*cexpr`: CurrentOfExpr structure containing the cursor name or parameter reference
- `*econtext`: Expression evaluation context containing parameter values if needed
- `table_oid`: OID of the target table for the CURRENT OF operation
- `current_tid`: Output parameter to receive the TID of the current row
## Dependencies
- Functions called/Symbols referenced:
  - [fetch_cursor_param_value](../f/fetch_cursor_param_value.md) (for parameterized cursor names)
  - [get_rel_name](../g/get_rel_name.md) (for error messages)
  - [GetPortalByName](../G/GetPortalByName.md) (to locate the cursor portal)
  - PortalIsValid (cursor validation)
  - [search_plan_tree](../s/search_plan_tree.md) (to find scan nodes in non-FOR-UPDATE cases)
  - [ItemPointerIsValid](../I/ItemPointerIsValid.md) (TID validation)
  - [slot_getsysattr](../s/slot_getsysattr.md) (to extract TID from tuple slots)
- Called from (representative examples):
  - [TidListEval](../T/TidListEval.md) (in nodeTidscan.c for TID scan execution)

## Notes and Other Information
The function returns true if a row was successfully identified, false if the cursor is valid for the table but not currently scanning a row of that table (legal in inheritance scenarios). It raises errors for invalid cursors, non-SELECT queries, held cursors, or cursors not positioned on rows. The implementation carefully handles both indexed-only scans (where TID comes from xs_heaptid) and regular scans (where TID is extracted from the tuple's system attributes).

## Simplified Source
```c
bool
execCurrentOf(CurrentOfExpr *cexpr, ExprContext *econtext, Oid table_oid, ItemPointer current_tid)
{
    char *cursor_name;
    Portal portal;
    QueryDesc *queryDesc;

    // Get cursor name (from direct reference or parameter)
    if (cexpr->cursor_name)
        cursor_name = cexpr->cursor_name;
    else
        cursor_name = fetch_cursor_param_value(econtext, cexpr->cursor_param);

    // Find and validate the cursor
    portal = GetPortalByName(cursor_name);
    if (!PortalIsValid(portal))
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_CURSOR),
                errmsg("cursor \"%s\" does not exist", cursor_name)));

    // Ensure cursor is a SELECT query and not held
    if (portal->strategy != PORTAL_ONE_SELECT)
        ereport(ERROR, (errcode(ERRCODE_INVALID_CURSOR_STATE),
                errmsg("cursor \"%s\" is not a SELECT query", cursor_name)));

    queryDesc = portal->queryDesc;
    if (queryDesc == NULL || queryDesc->estate == NULL)
        ereport(ERROR, (errcode(ERRCODE_INVALID_CURSOR_STATE),
                errmsg("cursor \"%s\" is held from a previous transaction", cursor_name)));

    // Check cursor is positioned on a row
    if (portal->atStart || portal->atEnd)
        ereport(ERROR, (errcode(ERRCODE_INVALID_CURSOR_STATE),
                errmsg("cursor \"%s\" is not positioned on a row", cursor_name)));

    // Two strategies: FOR UPDATE/SHARE vs regular scans
    if (queryDesc->estate->es_rowmarks)
    {
        // FOR UPDATE/SHARE strategy: find the row mark for this table
        ExecRowMark *erm = NULL;
        for (Index i = 0; i < queryDesc->estate->es_range_table_size; i++)
        {
            ExecRowMark *thiserm = queryDesc->estate->es_rowmarks[i];
            if (thiserm && RowMarkRequiresRowShareLock(thiserm->markType) &&
                thiserm->relid == table_oid)
            {
                if (erm)
                    ereport(ERROR, (errcode(ERRCODE_INVALID_CURSOR_STATE),
                            errmsg("cursor has multiple FOR UPDATE/SHARE references")));
                erm = thiserm;
            }
        }

        if (!erm)
            ereport(ERROR, (errcode(ERRCODE_INVALID_CURSOR_STATE),
                    errmsg("cursor does not have FOR UPDATE/SHARE reference to table")));

        // Return current TID if valid
        if (ItemPointerIsValid(&(erm->curCtid)))
        {
            *current_tid = erm->curCtid;
            return true;
        }
        return false;
    }
    else
    {
        // Non-FOR-UPDATE strategy: search plan tree for scan node
        ScanState *scanstate;
        bool pending_rescan = false;

        scanstate = search_plan_tree(queryDesc->planstate, table_oid, &pending_rescan);
        if (!scanstate)
            ereport(ERROR, (errcode(ERRCODE_INVALID_CURSOR_STATE),
                    errmsg("cursor is not a simply updatable scan")));

        // Check if scan is active
        if (TupIsNull(scanstate->ss_ScanTupleSlot) || pending_rescan)
            return false;

        // Extract TID from scan state
        if (IsA(scanstate, IndexOnlyScanState))
        {
            IndexScanDesc scan = ((IndexOnlyScanState *) scanstate)->ioss_ScanDesc;
            *current_tid = scan->xs_heaptid;
        }
        else
        {
            // Get TID from tuple's system attributes
            Datum ldatum;
            bool lisnull;
            ItemPointer tuple_tid;

            ldatum = slot_getsysattr(scanstate->ss_ScanTupleSlot,
                                   SelfItemPointerAttributeNumber, &lisnull);
            if (lisnull)
                ereport(ERROR, (errcode(ERRCODE_INVALID_CURSOR_STATE),
                        errmsg("cursor is not a simply updatable scan")));

            tuple_tid = (ItemPointer) DatumGetPointer(ldatum);
            *current_tid = *tuple_tid;
        }

        return true;
    }
}
```