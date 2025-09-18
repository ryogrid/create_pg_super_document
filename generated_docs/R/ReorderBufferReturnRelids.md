# ReorderBufferReturnRelids

## Location
[src/backend/replication/logical/reorderbuffer.c:637-648](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/reorderbuffer.c#L637-L648)

## Overview
Frees an array of relation IDs that was previously allocated by ReorderBufferGetRelids().

## Definition
```c
void ReorderBufferReturnRelids(ReorderBuffer *rb, Oid *relids)
```

## Detailed Description
ReorderBufferReturnRelids deallocates memory for an array of Oid values that was previously allocated by ReorderBufferGetRelids(). This function serves as the cleanup counterpart for relation ID arrays used in TRUNCATE operations during logical replication. Like its allocation counterpart, it's a simple wrapper around pfree() that maintains the abstraction layer for relation ID memory management in the reorder buffer system.

## Parameters / Member Variables
- `rb`: Pointer to the ReorderBuffer (parameter included for API consistency, though not used in the current implementation)
- `relids`: The array of Oid values to be freed (must have been allocated by ReorderBufferGetRelids)

## Dependencies
- Functions called/Symbols referenced:
  - [pfree](../p/pfree.md)
- Called from (representative examples):
  - [ReorderBufferReturnChange](ReorderBufferReturnChange.md)

## Notes and Other Information
This function should only be called on Oid arrays that were allocated using ReorderBufferGetRelids(). The ReorderBuffer parameter is included for API consistency and potential future use, though the current implementation only performs a simple pfree() operation. It's part of the memory management pair for handling relation ID arrays in TRUNCATE operations during logical replication.