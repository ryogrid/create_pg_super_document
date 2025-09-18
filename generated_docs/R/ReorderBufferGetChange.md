# ReorderBufferGetChange

## Location
[src/backend/replication/logical/reorderbuffer.c:503-517](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/reorderbuffer.c#L503-L517)

## Overview
Allocates and initializes a new ReorderBufferChange structure from the reorder buffer's change memory context.

## Definition


## Detailed Description
ReorderBufferGetChange creates a fresh ReorderBufferChange instance by allocating memory from the reorder buffer's specialized change context (change_context). It allocates the exact size needed for a ReorderBufferChange structure and initializes all fields to zero, providing a clean state for representing database changes during logical decoding. This function is used extensively throughout the logical decoding process to create change records for various types of database operations.

## Parameters / Member Variables
- `rb`: Pointer to the ReorderBuffer from which to allocate the new change

## Dependencies
- Functions called/Symbols referenced:
  - [MemoryContextAlloc](../M/MemoryContextAlloc.md)
  - memset (for zero-initialization)
- Called from (representative examples):
  - [DecodeInsert](../D/DecodeInsert.md)
  - [DecodeUpdate](../D/DecodeUpdate.md)  
  - [DecodeDelete](../D/DecodeDelete.md)
  - [DecodeTruncate](../D/DecodeTruncate.md)
  - [DecodeMultiInsert](../D/DecodeMultiInsert.md)
  - [DecodeSpecConfirm](../D/DecodeSpecConfirm.md)
  - [ReorderBufferQueueMessage](ReorderBufferQueueMessage.md)
  - ReorderBufferAddSnapshot
  - ReorderBufferAddNewCommandId
  - ReorderBufferAddNewTupleCids
  - ReorderBufferQueueInvalidations
  - [ReorderBufferRestoreChange](ReorderBufferRestoreChange.md)

## Notes and Other Information
- Uses the change_context slab allocator for efficient memory management of fixed-size change structures
- Zeroes out the entire structure to ensure all fields start with default/null values
- This is a public API function (not static) used extensively by decode.c and other reorderbuffer operations
- Memory is automatically freed when the reorder buffer's context is deleted
- Changes allocated by this function are typically added to transaction change lists
- The function is simple but critical for logical decoding performance due to frequent allocation of change records