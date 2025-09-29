# ExtendBufferedRel

## Location
[src/backend/storage/buffer/bufmgr.c:845-876](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/buffer/bufmgr.c#L845-L876)

## Overview
A convenience wrapper function that extends a buffered relation by exactly one block, providing a simplified interface to the more general ExtendBufferedRelBy function.

## Definition
```c
Buffer ExtendBufferedRel(BufferManagerRelation bmr,
                         ForkNumber forkNum,
                         BufferAccessStrategy strategy,
                         uint32 flags)
```

## Detailed Description
ExtendBufferedRel serves as a streamlined interface for extending a relation by a single block. This function is commonly used throughout PostgreSQL indexing and storage subsystems when incremental growth is needed. It internally calls ExtendBufferedRelBy with extend_by set to 1, handling the complexity of the more general extension mechanism while providing a simple single-block extension interface.

This function is particularly useful in index construction, page allocation for various access methods (B-tree, GIN, GiST, SP-GiST, Hash, BRIN), and sequence management where relations need to grow by exactly one block at a time.

## Parameters / Member Variables
- `bmr`: BufferManagerRelation handle representing the relation to extend
- `forkNum`: Fork number specifying which fork of the relation to extend (main, FSM, VM, etc.)
- `strategy`: BufferAccessStrategy for controlling buffer replacement behavior, can be NULL for default strategy
- `flags`: Control flags for the extension operation (e.g., EB_SKIP_EXTENSION_LOCK, EB_CLEAR_SIZE_CACHE)

## Dependencies
- Functions called/Symbols referenced:
  - [ExtendBufferedRelBy](ExtendBufferedRelBy.md)
  - [BufferManagerRelation](../B/BufferManagerRelation.md)
  - [BufferAccessStrategy](../B/BufferAccessStrategy.md)
- Called from (representative examples):
  - [brinbuild](../b/brinbuild.md)
  - [brinbuildempty](../b/brinbuildempty.md)
  - [revmap_physical_extend](../r/revmap_physical_extend.md)
  - [ginbuildempty](../g/ginbuildempty.md)
  - [GinNewBuffer](../G/GinNewBuffer.md)
  - [gistbuildempty](../g/gistbuildempty.md)
  - [gistNewBuffer](../g/gistNewBuffer.md)
  - [_hash_getnewbuf](../h/_hash_getnewbuf.md)
  - [_bt_allocbuf](../b/_bt_allocbuf.md)
  - [SpGistNewBuffer](../S/SpGistNewBuffer.md)
  - [fill_seq_fork_with_data](../f/fill_seq_fork_with_data.md)
  - [ReadBuffer_common](../R/ReadBuffer_common.md)

## Notes and Other Information
- This is a convenience wrapper that always extends by exactly one block
- Widely used across PostgreSQL access methods for page allocation
- The returned Buffer represents the newly allocated block
- Commonly used during index builds and maintenance operations
- Part of the buffered relation extension API introduced for more efficient bulk operations

## Simplified Source

```c
Buffer
ExtendBufferedRel(BufferManagerRelation bmr,
                  ForkNumber forkNum,
                  BufferAccessStrategy strategy,
                  uint32 flags)
{
    Buffer buf;
    uint32 extend_by = 1;

    // Extend the relation by exactly one block
    ExtendBufferedRelBy(bmr, forkNum, strategy, flags, extend_by,
                        &buf, &extend_by);

    return buf;
}
```