# TransitionCaptureState

## Location
src/include/commands/trigger.h: 56 - 82

## Overview
TransitionCaptureState manages the capture and storage of old and new tuples into transition tables for statement-level triggers during DML operations.

## Definition

```c
typedef struct TransitionCaptureState
{
	/*
	 * Is there at least one trigger specifying each transition relation on
	 * the relation explicitly named in the DML statement or COPY command?
	 * Note: in current usage, these flags could be part of the private state,
	 * but it seems possibly useful to let callers see them.
	 */
	bool		tcs_delete_old_table;
	bool		tcs_update_old_table;
	bool		tcs_update_new_table;
	bool		tcs_insert_new_table;

	/*
	 * For INSERT and COPY, it would be wasteful to convert tuples from child
	 * format to parent format after they have already been converted in the
	 * opposite direction during routing.  In that case we bypass conversion
	 * and allow the inserting code (copyfrom.c and nodeModifyTable.c) to
	 * provide a slot containing the original tuple directly.
	 */
	TupleTableSlot *tcs_original_insert_tuple;

	/*
	 * Private data including the tuplestore(s) into which to insert tuples.
	 */
	struct AfterTriggersTableData *tcs_private;
} TransitionCaptureState;
```
## Detailed Description
TransitionCaptureState is a per-caller state structure that coordinates the capture of tuples into transition tables for statement-level AFTER triggers. It determines which types of transition tables (OLD/NEW for different operations) are needed and manages the efficient collection of tuples during DML operations like INSERT, UPDATE, DELETE, and MERGE.

The structure is designed to handle complex scenarios involving table inheritance hierarchies, where tuple format conversions between child and parent tables need to be optimized. It also ensures that per SQL specification, all operations of the same kind on the same table during one query share a single transition table through shared AfterTriggersTableData structures.

## Parameters / Member Variables
- : Flag indicating whether any trigger requires an OLD transition table for DELETE operations
- : Flag indicating whether any trigger requires an OLD transition table for UPDATE operations  
- : Flag indicating whether any trigger requires a NEW transition table for UPDATE operations
- : Flag indicating whether any trigger requires a NEW transition table for INSERT operations
- : TupleTableSlot containing the original tuple format for INSERT/COPY operations to avoid redundant format conversions in inheritance scenarios
- : Pointer to AfterTriggersTableData containing the actual tuplestore(s) and shared state across multiple callers

## Dependencies
- Functions called/Symbols referenced:
  - TupleTableSlot
  - AfterTriggersTableData

- Called from (representative examples):
  - MakeTransitionCaptureState
  - ExecASInsertTriggers
  - ExecARInsertTriggers
  - ExecASDeleteTriggers
  - ExecARDeleteTriggers
  - ExecASUpdateTriggers
  - ExecARUpdateTriggers
  - GetAfterTriggersStoreSlot
  - GetAfterTriggersTransitionTable
  - TransitionTableAddTuple
  - AfterTriggerSaveEvent
  - ExecInsert
  - ExecDeleteEpilogue
  - ModifyTableState (as member)

## Notes and Other Information
- This structure is per-caller to avoid conflicts when setting tcs_original_insert_tuple, though the underlying private data may be shared across multiple callers
- Only used for statement-level AFTER triggers that specify transition table names (OLD/NEW TABLE AS ...)
- The structure optimizes tuple format handling in inheritance hierarchies by allowing direct provision of original tuple formats
- Transition tables are implemented using tuplestores managed by the transaction's resource owner
- The flags (tcs_*_table) allow callers to determine which types of transition tables are actively being collected
- Per SQL specification compliance, operations of the same type on the same table share transition table storage during a single query
- The structure coordinates with AfterTriggersTableData to ensure proper lifecycle management of transition table storage