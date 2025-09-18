# heapam_tuple_insert_speculative

## Location
src/backend/access/heap/heapam_handler.c: 261 - 283

## Overview
This function performs a speculative tuple insertion into a heap relation, which allows for tentative insertions that can be later confirmed or rolled back, commonly used for implementing unique constraint checking and conflict resolution.

## Definition
```c
static void
heapam_tuple_insert_speculative(Relation relation, TupleTableSlot *slot,
                                CommandId cid, int options,
                                BulkInsertState bistate, uint32 specToken)
```

## Detailed Description
heapam_tuple_insert_speculative is a static callback function used by the heap access method to perform speculative tuple insertions. Unlike regular insertions, speculative insertions are tentative and can be either confirmed or aborted based on subsequent operations, typically used for handling unique constraint violations and implementing upsert-like operations.

The function is similar to heapam_tuple_insert but adds speculative insertion support by setting a speculation token in the tuple header and adding the HEAP_INSERT_SPECULATIVE flag. The speculation token allows the system to later identify and manage the speculative tuple, enabling operations like confirming the insertion or rolling it back if conflicts are detected.

## Parameters / Member Variables
- `relation`: The heap relation into which the tuple will be speculatively inserted
- `slot`: TupleTableSlot containing the tuple data to insert
- `cid`: CommandId representing the command that initiated this insertion (for MVCC purposes)
- `options`: Integer flags controlling insertion behavior (HEAP_INSERT_SPECULATIVE will be added)
- `bistate`: BulkInsertState for optimizing bulk insertion operations (can be NULL for single inserts)
- `specToken`: uint32 speculation token used to identify and manage this speculative insertion

## Dependencies
- Functions called/Symbols referenced:
  - CommandId (parameter type)
  - [BulkInsertState](../B/BulkInsertState.md) (parameter type)
  - [ExecFetchSlotHeapTuple](../E/ExecFetchSlotHeapTuple.md) (slot-to-tuple conversion)
  - RelationGetRelid (relation OID retrieval)
  - HeapTupleHeaderSetSpeculativeToken (speculation token setting)
  - HEAP_INSERT_SPECULATIVE (flag constant)
  - [heap_insert](heap_insert.md) (core insertion logic)
  - [ItemPointerCopy](../I/ItemPointerCopy.md) (TID copying)
  - [pfree](../p/pfree.md) (memory deallocation)
- Called from (representative examples):
  - [SampleHeapTupleVisible](../S/SampleHeapTupleVisible.md) (indirectly through table AM interface)

## Notes and Other Information
- This is a static function serving as a callback in the table access method interface
- Speculative insertions are used primarily for ON CONFLICT handling and unique constraint checking
- The speculation token allows the system to later confirm or abort the speculative insertion
- The HEAP_INSERT_SPECULATIVE flag is automatically added to the provided options
- Memory management follows the same pattern as regular insertion with shouldFree checking
- Updates both slot and tuple with the relation's OID like regular insertion
- Part of PostgreSQL's infrastructure for handling insertion conflicts and implementing upsert operations
- The speculative tuple remains tentative until explicitly confirmed or rolled back
- Enables more efficient handling of unique constraint violations compared to traditional insert-then-check approaches