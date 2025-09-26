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