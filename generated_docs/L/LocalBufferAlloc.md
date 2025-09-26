# LocalBufferAlloc

## Location
[src/backend/storage/buffer/localbuf.c:116-176](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/buffer/localbuf.c#L116-L176)

## Overview
LocalBufferAlloc finds or creates a local buffer for a specified page of a temporary relation, serving as the local buffer equivalent of BufferAlloc for non-shared temporary relations.

## Definition

```c
BufferDesc *
LocalBufferAlloc(SMgrRelation smgr, ForkNumber forkNum, BlockNumber blockNum,
				 bool *foundPtr)
```
## Detailed Description
LocalBufferAlloc implements the core allocation logic for local buffers used by temporary relations. The function first searches the local buffer hash table to determine if the requested block is already cached. If found, it pins the existing buffer and returns it. If not found, it obtains a victim buffer through GetLocalVictimBuffer(), initializes it with the new block's identity, and updates the buffer's state flags to mark it as valid with initial usage count.

Unlike the shared buffer equivalent, LocalBufferAlloc doesn't require complex locking since local buffers are private to each backend process. The function ensures resource ownership tracking and maintains the buffer hash table consistency.

## Parameters
- : Storage manager relation handle for the temporary relation
- : Fork number specifying which fork of the relation (main, FSM, visibility map, etc.)
- : Block number within the specified fork to allocate
- : Output parameter indicating whether the block was already in the buffer cache

## Dependencies
- Functions called/Symbols referenced:
  - [InitBufferTag](../I/InitBufferTag.md): Creates buffer tag for the requested block
  - [InitLocalBuffers](../I/InitLocalBuffers.md): Initializes local buffer system if not already done
  - [ResourceOwnerEnlarge](../R/ResourceOwnerEnlarge.md): Ensures resource owner can track additional buffer
  - [hash_search](../h/hash_search.md): Searches/inserts entries in local buffer hash table
  - [GetLocalVictimBuffer](../G/GetLocalVictimBuffer.md): Selects a buffer for replacement when needed
  - [GetLocalBufferDescriptor](../G/GetLocalBufferDescriptor.md): Converts buffer ID to BufferDesc pointer
  - [PinLocalBuffer](../P/PinLocalBuffer.md): Pins an existing buffer and updates its state
  - [BufferTagsEqual](../B/BufferTagsEqual.md): Verifies buffer tag matches expected block identity
  - [pg_atomic_read_u32](../p/pg_atomic_read_u32.md)/pg_atomic_unlocked_write_u32: Atomic state manipulation
- Called from (representative examples):
  - [PinBufferForBlock](../P/PinBufferForBlock.md): Main buffer acquisition function delegates to this for temporary relations
  - [ResourceOwnerForgetBufferIO](../R/ResourceOwnerForgetBufferIO.md): Buffer resource management

## Notes and Other Information
- No locking required since local buffers are backend-private
- Always uses default access strategy (usage count is always advanced)
- Buffer state flags are atomically updated to maintain consistency
- Uses BM_TAG_VALID and BUF_USAGECOUNT_ONE flags when initializing new buffers
- The negative buffer ID encoding (-victim_buffer - 1) follows PostgreSQL's convention for local buffer identification
- [Hash](../H/Hash.md) table corruption detection includes assertion that shouldn't normally trigger
- Part of PostgreSQL's local buffer management system optimized for temporary relations