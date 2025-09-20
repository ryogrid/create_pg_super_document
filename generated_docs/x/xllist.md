# xllist

## Location
[src/backend/access/transam/twophase.c:1002-1020](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/twophase.c#L1002-L1020)

## Overview
A static data structure that manages a chain of StateFileChunk blocks used for assembling two-phase commit state files in memory before writing them to WAL and disk.

## Definition

```c
struct xllist
{
	StateFileChunk *head;		/* first data block in the chain */
	StateFileChunk *tail;		/* last block in chain */
	uint32		num_chunks;
	uint32		bytes_free;		/* free bytes left in tail block */
	uint32		total_len;		/* total data bytes in chain */
}			records;
```
## Detailed Description
The  structure (instantiated as ) is a core component of PostgreSQL's two-phase commit implementation. It serves as a temporary container for assembling state file data during the prepare phase of a two-phase commit transaction. The structure manages a linked list of StateFileChunk blocks, each containing portions of the serialized transaction state data.

The structure is designed to efficiently handle variable-length data by maintaining both head and tail pointers for O(1) append operations, tracking the total number of chunks and bytes for validation, and maintaining free space information to minimize memory allocation calls.

## Parameters / Member Variables
- `*head`: Pointer to the first StateFileChunk in the linked list chain
- `*tail`: Pointer to the last StateFileChunk in the chain, used for efficient appending
- `num_chunks`: Count of StateFileChunk blocks currently in the chain
- `bytes_free`: Number of free bytes remaining in the tail block for data storage
- `total_len`: Total number of data bytes stored across all chunks in the chain

## Dependencies
- Functions called/Symbols referenced:
  - [StateFileChunk](../S/StateFileChunk.md) (struct type for individual data blocks)

- Called from (representative examples):
  - [save_state_data](../s/save_state_data.md) (appends data to the records structure)
  - [StartPrepare](../S/StartPrepare.md) (initializes the records structure)
  - [EndPrepare](../E/EndPrepare.md) (finalizes and processes the records structure)

## Notes and Other Information
- The structure is declared static and instantiated as , making it a file-scope global variable
- Used exclusively during two-phase commit preparation (src/backend/access/transam/twophase.c:1002-1009)
- Data blocks are padded to MAXALIGN boundaries for proper memory alignment
- The structure enforces a maximum size limit (MaxAllocSize) to prevent excessive memory usage
- Memory management follows PostgreSQL's palloc/pfree allocation model
- The linked list design allows for incremental data assembly without requiring upfront size knowledge