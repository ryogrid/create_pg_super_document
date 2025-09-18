# heapam_tuple_insert

## Location
[src/backend/access/heap/heapam_handler.c:242-260](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/heap/heapam_handler.c#L242-L260)

## Overview
This function inserts a tuple from a table slot into a heap relation, serving as the heap access method's callback for tuple insertion operations with proper tuple preparation and result handling.

## Definition
```c
static void
heapam_tuple_insert(Relation relation, TupleTableSlot *slot, CommandId cid,
                    int options, BulkInsertState bistate)
```

## Detailed Description
heapam_tuple_insert is a static callback function used by the heap access method to insert tuples into heap relations. The function acts as a wrapper around the lower-level heap_insert function, handling the conversion between the table access method's slot-based interface and the heap access method's tuple-based interface.

The function extracts a HeapTuple from the provided slot, sets the appropriate table OID information, performs the actual insertion through heap_insert, and then updates the slot with the resulting tuple identifier (TID). It also manages memory allocation by freeing the tuple if it was allocated during the slot-to-tuple conversion.

## Parameters / Member Variables
- `relation`: The heap relation into which the tuple will be inserted
- `slot`: TupleTableSlot containing the tuple data to insert
- `cid`: CommandId representing the command that initiated this insertion (for MVCC purposes)
- `options`: Integer flags controlling insertion behavior (passed through to heap_insert)
- `bistate`: BulkInsertState for optimizing bulk insertion operations (can be NULL for single inserts)

## Dependencies
- Functions called/Symbols referenced:
  - CommandId (parameter type)
  - [BulkInsertState](../B/BulkInsertState.md) (parameter type)
  - [ExecFetchSlotHeapTuple](../E/ExecFetchSlotHeapTuple.md) (slot-to-tuple conversion)
  - RelationGetRelid (relation OID retrieval)
  - [heap_insert](heap_insert.md) (core insertion logic)
  - [ItemPointerCopy](../I/ItemPointerCopy.md) (TID copying)
  - [pfree](../p/pfree.md) (memory deallocation)
- Called from (representative examples):
  - [SampleHeapTupleVisible](../S/SampleHeapTupleVisible.md) (indirectly through table AM interface)

## Notes and Other Information
- This is a static function serving as a callback in the table access method interface
- The function handles memory management by checking shouldFree flag and freeing the tuple if necessary
- Updates both the slot's tts_tableOid and the tuple's t_tableOid with the relation's OID
- Copies the resulting TID from the inserted tuple back to the slot for caller access
- Part of the heap access method's tuple manipulation functions
- The options parameter can include flags like HEAP_INSERT_SKIP_WAL, HEAP_INSERT_SKIP_FSM, etc.
- [BulkInsertState](../B/BulkInsertState.md) parameter enables optimization for bulk operations but is optional for single inserts
- The function ensures proper integration between the generic table access method layer and heap-specific implementation