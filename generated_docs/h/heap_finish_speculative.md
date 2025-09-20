# heap_finish_speculative

## Location
[src/backend/access/heap/heapam.c:6042-6128](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/heap/heapam.c#L6042-L6128)

## Overview
Marks a speculative heap tuple insertion as successful by replacing the speculative token with a proper t_ctid pointing to itself.

## Definition

```c
void
heap_finish_speculative(Relation relation, ItemPointer tid)
```
## Detailed Description
This function completes a speculative insertion by converting the speculative token stored in the tuple's t_ctid field into a proper self-referencing pointer, which is the standard format for newly inserted ordinary tuples. 

The function performs the following operations:
1. Reads and locks the buffer containing the tuple
2. Validates that the tuple exists and is in speculative state
3. Replaces the speculative token in t_ctid with the tuple's own ItemPointer
4. Logs the confirmation operation via WAL if needed
5. Releases the buffer

This operation is critical for UPSERT functionality where speculative insertions need to be either confirmed or aborted based on conflict detection.

## Parameters / Member Variables
- : The heap relation containing the speculative tuple
- : ItemPointer identifying the location of the speculative tuple to be confirmed

## Dependencies
- Functions called/Symbols referenced:
  - [ReadBuffer](../R/ReadBuffer.md)
  - [ItemPointerGetBlockNumber](../I/ItemPointerGetBlockNumber.md)
  - [ItemPointerGetOffsetNumber](../I/ItemPointerGetOffsetNumber.md)  
  - [PageGetMaxOffsetNumber](../P/PageGetMaxOffsetNumber.md)
  - [PageGetItemId](../P/PageGetItemId.md)
  - ItemIdIsNormal
  - [PageGetItem](../P/PageGetItem.md)
  - HeapTupleHeaderIsSpeculative
  - MarkBufferDirty
  - RelationNeedsWAL
  - [XLogBeginInsert](../X/XLogBeginInsert.md)
  - [XLogSetRecordFlags](../X/XLogSetRecordFlags.md)
  - [XLogRegisterData](../X/XLogRegisterData.md)
  - [XLogRegisterBuffer](../X/XLogRegisterBuffer.md)
  - [XLogInsert](../X/XLogInsert.md)
  - [PageSetLSN](../P/PageSetLSN.md)
  - [UnlockReleaseBuffer](../U/UnlockReleaseBuffer.md)
- Called from (representative examples):
  - [heapam_tuple_complete_speculative](heapam_tuple_complete_speculative.md)
  - HeapScanIsValid (indirect reference)

## Notes and Other Information
- Must be called within a critical section to ensure atomic completion
- Generates XLOG_HEAP_CONFIRM WAL record for crash recovery
- Part of PostgreSQL's speculative insertion mechanism used in UPSERT operations
- It is mandatory to either finish or abort every speculative insertion - leaving them uncommitted is not permitted
- The function assumes the tuple is already validated as speculative via HeapTupleHeaderIsSpeculative assertion