# pgoutput_row_filter

## Location
[src/backend/replication/pgoutput/pgoutput.c:1248-1428](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/pgoutput/pgoutput.c#L1248-L1428)

## Overview
Evaluates row filter expressions to determine if a tuple change should be replicated, potentially transforming UPDATE operations into INSERT or DELETE based on filter match results.

## Definition

```c
enums
	 * having specific values.
	 */
	static const int map_changetype_pubaction[] = {
		[REORDER_BUFFER_CHANGE_INSERT] = PUBACTION_INSERT,
		[REORDER_BUFFER_CHANGE_UPDATE] = PUBACTION_UPDATE,
		[REORDER_BUFFER_CHANGE_DELETE] = PUBACTION_DELETE
	};
```
## Detailed Description
This function implements sophisticated row filtering logic for logical replication in the pgoutput plugin. It evaluates row filter expressions against old and new tuple versions to determine if changes should be replicated. For INSERT and DELETE operations, it simply evaluates the filter against the single available tuple. For UPDATE operations, it implements complex transformation logic: if only the old tuple matches, it converts UPDATE to DELETE; if only the new tuple matches, it converts UPDATE to INSERT; if both match, it keeps the UPDATE; if neither matches, it drops the change entirely. The function also handles TOAST (The Oversized-Attribute Storage Technique) values by copying unchanged replica identity columns from old to new tuples when necessary.

## Parameters / Member Variables
- : The relation being replicated
- : Tuple table slot containing the old tuple version (for UPDATE/DELETE)
- : Pointer to tuple table slot containing the new tuple version (for INSERT/UPDATE), may be modified for transformations
- : RelationSyncEntry containing the prepared row filter expression states
- : Pointer to ReorderBufferChangeType that may be modified to transform the operation type

## Dependencies
- Functions called/Symbols referenced:
  - [get_namespace_name](../g/get_namespace_name.md)
  - RelationGetNamespace
  - RelationGetRelationName
  - ResetPerTupleExprContext
  - GetPerTupleExprContext
  - [pgoutput_row_filter_exec_expr](pgoutput_row_filter_exec_expr.md)
  - [slot_getallattrs](../s/slot_getallattrs.md)
  - RelationGetDescr
  - TupleDescAttr
  - [MakeSingleTupleTableSlot](../M/MakeSingleTupleTableSlot.md)
  - [ExecClearTuple](../E/ExecClearTuple.md)
  - [ExecStoreVirtualTuple](../E/ExecStoreVirtualTuple.md)
  - VARATT_IS_EXTERNAL_ONDISK
  - TTSOpsVirtual
  - PUBACTION_INSERT/UPDATE/DELETE constants
  - REORDER_BUFFER_CHANGE_INSERT/UPDATE/DELETE constants
- Called from (representative examples):
  - [pgoutput_change](pgoutput_change.md)

## Notes and Other Information
- Returns true if the change should be replicated, false if it should be dropped
- Implements a four-case transformation matrix for UPDATE operations based on old/new tuple filter matches
- Handles TOAST values by copying unchanged replica identity columns from old to new tuples
- Uses a mapping array to convert ReorderBufferChangeType to publication action types
- Creates temporary tuple slots when needed to ensure all replica identity columns are available
- Critical for maintaining data consistency when row filters are used in logical replication
- Includes extensive debugging support with elog(DEBUG3) messages
- Static function only accessible within pgoutput.c
- Essential component of selective row replication functionality

## Simplified Source

```c
static bool
pgoutput_row_filter(Relation relation, TupleTableSlot *old_slot,
                    TupleTableSlot **new_slot_ptr, RelationSyncEntry *entry,
                    ReorderBufferChangeType *action)
{
    // Map change types to publication actions
    static const int map_changetype_pubaction[] = {
        [REORDER_BUFFER_CHANGE_INSERT] = PUBACTION_INSERT,
        [REORDER_BUFFER_CHANGE_UPDATE] = PUBACTION_UPDATE,
        [REORDER_BUFFER_CHANGE_DELETE] = PUBACTION_DELETE
    };

    TupleTableSlot *new_slot = *new_slot_ptr;
    ExprState *filter_exprstate = entry->exprstate[map_changetype_pubaction[*action]];

    // No filter means allow all
    if (!filter_exprstate)
        return true;

    ExprContext *ecxt = GetPerTupleExprContext(entry->estate);
    ResetPerTupleExprContext(entry->estate);

    // Simple case: single tuple operations (INSERT, DELETE, or UPDATE with one tuple)
    if (!new_slot || !old_slot)
    {
        ecxt->ecxt_scantuple = new_slot ? new_slot : old_slot;
        return pgoutput_row_filter_exec_expr(filter_exprstate, ecxt);
    }

    // Complex case: UPDATE with both old and new tuples
    slot_getallattrs(new_slot);
    slot_getallattrs(old_slot);

    TupleDesc desc = RelationGetDescr(relation);
    TupleTableSlot *tmp_new_slot = NULL;

    // Handle TOAST values: copy unchanged replica identity columns
    for (int i = 0; i < desc->natts; i++)
    {
        Form_pg_attribute att = TupleDescAttr(desc, i);

        if (new_slot->tts_isnull[i] || old_slot->tts_isnull[i])
            continue;

        // Copy TOAST values from old to new if needed
        if (att->attlen == -1 &&
            VARATT_IS_EXTERNAL_ONDISK(new_slot->tts_values[i]) &&
            !VARATT_IS_EXTERNAL_ONDISK(old_slot->tts_values[i]))
        {
            if (!tmp_new_slot)
            {
                tmp_new_slot = MakeSingleTupleTableSlot(desc, &TTSOpsVirtual);
                ExecClearTuple(tmp_new_slot);
                memcpy(tmp_new_slot->tts_values, new_slot->tts_values,
                       desc->natts * sizeof(Datum));
                memcpy(tmp_new_slot->tts_isnull, new_slot->tts_isnull,
                       desc->natts * sizeof(bool));
            }
            tmp_new_slot->tts_values[i] = old_slot->tts_values[i];
            tmp_new_slot->tts_isnull[i] = old_slot->tts_isnull[i];
        }
    }

    // Evaluate filter for old tuple
    ecxt->ecxt_scantuple = old_slot;
    bool old_matched = pgoutput_row_filter_exec_expr(filter_exprstate, ecxt);

    // Evaluate filter for new tuple
    if (tmp_new_slot)
    {
        ExecStoreVirtualTuple(tmp_new_slot);
        ecxt->ecxt_scantuple = tmp_new_slot;
    }
    else
        ecxt->ecxt_scantuple = new_slot;

    bool new_matched = pgoutput_row_filter_exec_expr(filter_exprstate, ecxt);

    // Transform UPDATE based on filter results:
    // Case 1: old=false, new=false -> drop change
    if (!old_matched && !new_matched)
        return false;

    // Case 2: old=false, new=true -> convert to INSERT
    if (!old_matched && new_matched)
    {
        *action = REORDER_BUFFER_CHANGE_INSERT;
        if (tmp_new_slot)
            *new_slot_ptr = tmp_new_slot;
    }
    // Case 3: old=true, new=false -> convert to DELETE
    else if (old_matched && !new_matched)
        *action = REORDER_BUFFER_CHANGE_DELETE;

    // Case 4: old=true, new=true -> keep as UPDATE (no change needed)

    return true;
}
```