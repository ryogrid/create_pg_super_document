# StateFileChunk

## Location
[src/backend/access/transam/twophase.c:995-1000](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/twophase.c#L995-L1000)

## Overview
StateFileChunk is a linked list node structure used during the prepare phase to assemble two-phase commit state data in memory before writing it to WAL and the actual state file.

## Definition
```c
typedef struct StateFileChunk
{
    char       *data;
    uint32      len;
    struct StateFileChunk *next;
} StateFileChunk;
```

## Detailed Description
This structure implements a memory-efficient approach for building two-phase commit state files during the prepare phase. Rather than allocating one large contiguous block of memory, the system uses a chain of StateFileChunk blocks to incrementally assemble the state data.

This chunked approach provides several benefits:
1. **Memory Efficiency**: Avoids the need to pre-allocate large contiguous memory blocks
2. **Flexible Growth**: Allows the state file content to grow dynamically as different resource managers contribute their data
3. **Streaming Capability**: Enables efficient writing to WAL and disk by processing chunks sequentially

The linked list structure facilitates easy traversal during the writing phase, where each chunk's data is sequentially written to create the final state file.

## Parameters / Member Variables
- `data`: Pointer to a memory buffer containing a portion of the state file data for this chunk
- `len`: Length in bytes of the data stored in this chunk's buffer
- `next`: Pointer to the next StateFileChunk in the linked list, forming a chain of chunks that together represent the complete state file content

## Dependencies
- Functions called/Symbols referenced:
  - [StateFileChunk](StateFileChunk.md) (self-reference for linked list structure)
- Called from (representative examples):
  - [xllist](../x/xllist.md) (global variable maintaining the chunk list)
  - [save_state_data](../s/save_state_data.md) (for adding new chunks to the state data)
  - [StartPrepare](StartPrepare.md) (for initializing the chunk list during prepare)
  - [EndPrepare](../E/EndPrepare.md) (for processing and writing the assembled chunks)

## Notes and Other Information
- This is a temporary in-memory structure used only during the prepare phase of two-phase commit
- The chunk list is typically accessed through global variables like xllist during prepare operations
- Memory management for both the chunk structures and their data buffers must be carefully handled to prevent leaks
- The chunked approach allows for efficient streaming to both WAL records and state files without requiring large contiguous memory allocations
- This structure is part of PostgreSQL's optimization for handling large prepared transactions efficiently