# DestroyBlockRefTableReader

## Location
[src/common/blkreftable.c:773-789](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/blkreftable.c#L773-L789)

## Overview
Releases all memory allocated for a BlockRefTableReader structure, performing proper cleanup of dynamic allocations.

## Definition

```c
void
DestroyBlockRefTableReader(BlockRefTableReader *reader)
```
## Detailed Description
DestroyBlockRefTableReader performs complete cleanup of a BlockRefTableReader structure by releasing all dynamically allocated memory. The function first checks if the chunk_size array was allocated and frees it if necessary, then frees the reader structure itself. This function provides the symmetric cleanup operation for readers created by CreateBlockRefTableReader, ensuring no memory leaks occur when block reference table reading is complete.

## Parameters / Member Variables
- : Pointer to the BlockRefTableReader structure to be destroyed and deallocated

## Dependencies
- Functions called/Symbols referenced:
  - [pfree](../p/pfree.md)
- Called from (representative examples):
  - [PrepareForIncrementalBackup](../P/PrepareForIncrementalBackup.md)
  - [pg_wal_summary_contents](../p/pg_wal_summary_contents.md)

## Notes and Other Information
- Safely handles NULL chunk_size pointer by checking before freeing
- Sets chunk_size to NULL after freeing to prevent double-free errors
- Should be called after completing all block reference table reading operations
- Does not attempt to close or cleanup I/O resources (handled by callback functions)
- Essential for preventing memory leaks in applications using block reference table readers
- Simple cleanup function with no return value or error conditions