# ExtendBufferedRelShared

## Location
[src/backend/storage/buffer/bufmgr.c:2179-2458](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/buffer/bufmgr.c#L2179-L2458)

## Overview
ExtendBufferedRelShared extends shared persistent relations by acquiring victim buffers, coordinating with extension locks, and managing buffer table insertions for concurrent access.

## Definition

```c
static BlockNumber
ExtendBufferedRelShared(BufferManagerRelation bmr,
						ForkNumber fork,
						BufferAccessStrategy strategy,
						uint32 flags,
						uint32 extend_by,
						BlockNumber extend_upto,
						Buffer *buffers,
						uint32 *extended_by)
```
## Detailed Description
ExtendBufferedRelShared implements the complex logic for extending shared (persistent) relations in PostgreSQL. It operates in several phases: first acquiring victim buffers and zeroing them outside the extension lock to minimize lock hold time; then taking the extension lock and determining the actual extension size based on concurrent changes; inserting buffers into the buffer mapping table; performing the actual storage extension via smgrzeroextend; and finally marking buffers as valid and waking waiting backends.

The function handles several edge cases including concurrent extensions, existing buffers from failed previous attempts, and enforces relation size limits. It coordinates with buffer access strategies for victim buffer selection and includes comprehensive error handling for corrupted data scenarios. The implementation optimizes performance by doing expensive operations (victim buffer writeout, zeroing) before acquiring locks.

## Parameters / Member Variables
- : BufferManagerRelation containing relation metadata and storage manager
- : ForkNumber specifying which fork of the relation to extend (main, FSM, VM, etc.)
- : BufferAccessStrategy for buffer management policy and victim selection
- : uint32 controlling extension behavior (EB_SKIP_EXTENSION_LOCK, EB_CLEAR_SIZE_CACHE, EB_LOCK_FIRST, EB_LOCK_TARGET)
- : uint32 specifying the number of blocks to extend by (modified by LimitAdditionalPins)
- : BlockNumber specifying target block number to extend up to (InvalidBlockNumber for unlimited)
- : Buffer array to receive handles for newly allocated blocks
- : Pointer to uint32 that receives the actual number of blocks extended

## Dependencies
- Functions called/Symbols referenced:
  - [IOContextForStrategy](../I/IOContextForStrategy.md)
  - [LimitAdditionalPins](../L/LimitAdditionalPins.md)
  - [GetVictimBuffer](../G/GetVictimBuffer.md)
  - BufHdrGetBlock/GetBufferDescriptor
  - MemSet
  - [LockRelationForExtension](../L/LockRelationForExtension.md)/UnlockRelationForExtension
  - [smgrnblocks](../s/smgrnblocks.md)/smgrzeroextend
  - [BufTableInsert](../B/BufTableInsert.md)/BufTableHashCode
  - [BufferDescriptorGetContentLock](../B/BufferDescriptorGetContentLock.md)
  - [PinBuffer](../P/PinBuffer.md)/UnpinBuffer
  - [StartBufferIO](../S/StartBufferIO.md)/TerminateBufferIO
  - [StrategyFreeBuffer](../S/StrategyFreeBuffer.md)
  - [pgstat_count_io_op_time](../p/pgstat_count_io_op_time.md)
  - [PageIsNew](../P/PageIsNew.md)
- Called from (representative examples):
  - [ExtendBufferedRelCommon](ExtendBufferedRelCommon.md)

## Notes and Other Information
- Handles concurrent extension scenarios by rechecking relation size after acquiring lock
- Implements deadlock avoidance by doing expensive operations before lock acquisition  
- Supports partial extensions when extend_upto parameter limits the final size
- Enforces MaxBlockNumber limit to prevent relation overflow
- Includes comprehensive error handling for unexpected data beyond EOF
- Optimizes performance through careful lock ordering and batched operations
- Critical component of PostgreSQL's relation extension and buffer management system