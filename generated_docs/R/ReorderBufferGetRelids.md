# ReorderBufferGetRelids

## Location
[src/backend/replication/logical/reorderbuffer.c:621-636](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/reorderbuffer.c#L621-L636)

## Overview
Allocates and returns an array of Oid values to store relation IDs for truncated relations in logical replication.

## Definition
```c
Oid *ReorderBufferGetRelids(ReorderBuffer *rb, int nrelids)
```

## Detailed Description
ReorderBufferGetRelids allocates memory for an array of Oid values that will hold relation identifiers for TRUNCATE operations in logical replication. The function uses the ReorderBuffer's global context for allocation rather than specialized contexts like SLAB or tuple contexts, as TRUNCATE operations are relatively uncommon and don't warrant a dedicated memory context. The allocation size is calculated based on the number of relation IDs requested multiplied by the size of an Oid.

## Parameters / Member Variables
- `rb`: Pointer to the ReorderBuffer from which to allocate memory
- `nrelids`: Number of relation IDs for which to allocate space

## Dependencies
- Functions called/Symbols referenced:
  - [MemoryContextAlloc](../M/MemoryContextAlloc.md)
- Called from (representative examples):
  - [DecodeTruncate](../D/DecodeTruncate.md)
  - [ReorderBufferRestoreChange](ReorderBufferRestoreChange.md)

## Notes and Other Information
The function specifically uses the ReorderBuffer's global context (rb->context) rather than more specialized contexts. As noted in the comments, this choice is made because TRUNCATE is not a particularly common operation, making a dedicated context overkill. The SLAB contexts cannot be used for this purpose, and the tuple context is reserved for tuple data rather than relation ID arrays.