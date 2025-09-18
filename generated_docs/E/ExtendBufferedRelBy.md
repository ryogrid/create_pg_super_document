# ExtendBufferedRelBy

## Location
[src/backend/storage/buffer/bufmgr.c:877-908](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/buffer/bufmgr.c#L877-L908)

## Overview
Extends a buffered relation by multiple blocks, providing bulk extension capabilities with flexible buffer management and resource-aware allocation strategies.

## Definition
```c
BlockNumber ExtendBufferedRelBy(BufferManagerRelation bmr,
                                ForkNumber fork,
                                BufferAccessStrategy strategy,
                                uint32 flags,
                                uint32 extend_by,
                                Buffer *buffers,
                                uint32 *extended_by)
```

## Detailed Description
ExtendBufferedRelBy is the core function for bulk relation extension in PostgreSQL. It attempts to extend a relation by the requested number of blocks but may allocate fewer blocks depending on resource availability and system constraints. The function guarantees extending by at least one block unless an error occurs.

This function provides sophisticated buffer management by accepting an array of Buffer pointers that will be populated with pinned buffers for each newly allocated block. The actual number of blocks extended is returned through the extended_by parameter, allowing callers to handle partial extensions gracefully.

The function can work with either a Relation object (bmr.rel) or directly with a storage manager relation (bmr.smgr), automatically setting up the storage manager context when working with a Relation.

## Parameters / Member Variables
- `bmr`: BufferManagerRelation containing either a Relation pointer or SMgrRelation pointer with persistence info
- `fork`: Fork number specifying which fork of the relation to extend
- `strategy`: BufferAccessStrategy for buffer replacement policy, can be NULL for default behavior
- `flags`: Control flags such as EB_LOCK_FIRST to lock the first returned buffer
- `extend_by`: Requested number of blocks to extend (input)
- `buffers`: Array of Buffer pointers to receive pinned buffers for new blocks (must be at least extend_by elements)
- `extended_by`: Pointer to uint32 that receives the actual number of blocks extended (output)

## Dependencies
- Functions called/Symbols referenced:
  - RelationGetSmgr
  - [ExtendBufferedRelCommon](ExtendBufferedRelCommon.md)
  - [BufferManagerRelation](../B/BufferManagerRelation.md)
  - [BufferAccessStrategy](../B/BufferAccessStrategy.md)
- Called from (representative examples):
  - [ExtendBufferedRel](ExtendBufferedRel.md)
  - MAX_BUFFERS_TO_EXTEND_BY (heap access method)

## Notes and Other Information
- Guarantees extending by at least one block unless an error is thrown
- May extend by fewer blocks than requested due to resource constraints
- The buffers array must be pre-allocated and sized to at least extend_by elements
- EB_LOCK_FIRST flag ensures the first buffer is locked, useful for guaranteed empty buffers
- Automatically handles storage manager setup when working with Relation objects
- Part of the buffered relation extension API designed for efficient bulk operations
- Returns the starting block number of the extension