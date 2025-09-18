# ExtendBufferedRel

## Location
src/backend/storage/buffer/bufmgr.c: 845 - 876

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
  - ExtendBufferedRelBy
  - BufferManagerRelation
  - BufferAccessStrategy
- Called from (representative examples):
  - brinbuild
  - brinbuildempty
  - revmap_physical_extend
  - ginbuildempty
  - GinNewBuffer
  - gistbuildempty
  - gistNewBuffer
  - _hash_getnewbuf
  - _bt_allocbuf
  - SpGistNewBuffer
  - fill_seq_fork_with_data
  - ReadBuffer_common

## Notes and Other Information
- This is a convenience wrapper that always extends by exactly one block
- Widely used across PostgreSQL access methods for page allocation
- The returned Buffer represents the newly allocated block
- Commonly used during index builds and maintenance operations
- Part of the buffered relation extension API introduced for more efficient bulk operations