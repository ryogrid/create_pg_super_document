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
- : CurrentOfExpr structure containing the cursor name or parameter reference
- : Expression evaluation context containing parameter values if needed
- : OID of the target table for the CURRENT OF operation
- : Output parameter to receive the TID of the current row

## Dependencies
- Functions called/Symbols referenced:
  - [fetch_cursor_param_value](../f/fetch_cursor_param_value.md) (for parameterized cursor names)
  - [get_rel_name](../g/get_rel_name.md) (for error messages)
  - GetPortalByName (to locate the cursor portal)
  - PortalIsValid (cursor validation)
  - [search_plan_tree](../s/search_plan_tree.md) (to find scan nodes in non-FOR-UPDATE cases)
  - [ItemPointerIsValid](../I/ItemPointerIsValid.md) (TID validation)
  - slot_getsysattr (to extract TID from tuple slots)
- Called from (representative examples):
  - TidListEval (in nodeTidscan.c for TID scan execution)

## Notes and Other Information
The function returns true if a row was successfully identified, false if the cursor is valid for the table but not currently scanning a row of that table (legal in inheritance scenarios). It raises errors for invalid cursors, non-SELECT queries, held cursors, or cursors not positioned on rows. The implementation carefully handles both indexed-only scans (where TID comes from xs_heaptid) and regular scans (where TID is extracted from the tuple's system attributes).