# ReorderBufferRestoreChanges

## Location
[src/backend/replication/logical/reorderbuffer.c:4387-4529](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/reorderbuffer.c#L4387-L4529)

## Overview
ReorderBufferRestoreChanges restores a number of changes that were previously spilled to disk back into memory, managing the file I/O operations and memory allocation during the deserialization process.

## Definition
```c
static Size ReorderBufferRestoreChanges(ReorderBuffer *rb, ReorderBufferTXN *txn, TXNEntryFile *file, XLogSegNo *segno)
```

## Detailed Description
This function is responsible for reading serialized logical replication changes from disk and restoring them into memory structures. It operates as part of the memory management strategy for large transactions in logical replication, where changes are spilled to disk when memory usage exceeds configured limits. The function reads changes from multiple WAL segment-based files, handles file opening/closing, performs error checking, and deserializes the changes back into proper in-memory format.

The function works within memory limits (max_changes_in_memory) and processes changes across multiple WAL segments, opening and reading from spill files as needed. It first frees existing in-memory changes to make room, then reads serialized changes from disk files and restores them using ReorderBufferRestoreChange.

## Parameters / Member Variables
- `rb`: Pointer to the ReorderBuffer containing memory management context and buffers
- `txn`: Pointer to the ReorderBufferTXN transaction containing changes to be restored
- `file`: Pointer to TXNEntryFile structure managing the current spill file state
- `segno`: Pointer to XLogSegNo indicating the current WAL segment number being processed

## Dependencies
- Functions called/Symbols referenced:
  - [ReorderBufferReturnChange](ReorderBufferReturnChange.md) (to free existing changes)
  - XLByteToSeg (WAL segment calculation)
  - [ReorderBufferSerializedPath](ReorderBufferSerializedPath.md) (path generation for spill files)
  - PathNameOpenFile (file opening)
  - [ReorderBufferSerializeReserve](ReorderBufferSerializeReserve.md) (buffer management)
  - [FileRead](../F/FileRead.md) (disk I/O)
  - FileClose (file cleanup)
  - [ReorderBufferRestoreChange](ReorderBufferRestoreChange.md) (change deserialization)
  - dlist_* functions (doubly-linked list operations)
- Called from (representative examples):
  - [ReorderBufferIterTXNInit](ReorderBufferIterTXNInit.md)
  - [ReorderBufferIterTXNNext](ReorderBufferIterTXNNext.md)

## Notes and Other Information
- This is a static function used internally within the reorderbuffer.c module
- The function manages memory efficiently by freeing existing changes before loading new ones
- File I/O is performed using PostgreSQLs virtual file descriptor system
- The function handles multiple WAL segments and can process changes across segment boundaries
- Error handling includes comprehensive file I/O error reporting
- The function respects the max_changes_in_memory limit to prevent excessive memory usage
- Changes are read in their serialized ReorderBufferDiskChange format and then converted back to in-memory format