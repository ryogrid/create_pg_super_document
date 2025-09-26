# ExtendBufferedRelLocal

## Location
src/backend/storage/buffer/localbuf.c: 313 - 448

## Overview
ExtendBufferedRelLocal extends temporary relations by allocating new blocks and associated local buffers, serving as the local buffer implementation of ExtendBufferedRelBy() and ExtendBufferedRelTo().

## Definition


## Detailed Description
ExtendBufferedRelLocal implements relation extension for temporary relations using local buffers. The function performs several critical steps:

1. **Resource Management**: Limits the extension request using LimitAdditionalLocalPins() to prevent buffer exhaustion
2. **Buffer Allocation**: Obtains victim buffers through GetLocalVictimBuffer() and zero-initializes their contents
3. **Size Validation**: Checks current relation size and validates extension limits against MaxBlockNumber
4. **Hash Table Management**: For each new block, either reuses existing buffer entries or creates new hash table entries
5. **Physical Extension**: Performs actual disk space allocation via smgrzeroextend()
6. **State Finalization**: Sets BM_VALID flag on all extended buffers to mark them as ready for use

The function handles the complexity of coordinating buffer allocation, hash table updates, and physical storage extension while maintaining consistency between in-memory and on-disk state.

## Parameters
- : Buffer manager relation containing the storage manager relation handle
- : Fork number specifying which fork of the relation to extend
- : Extension flags controlling behavior (currently unused in local implementation)
- : Number of blocks to extend the relation by
- : Target block number for extension (used for validation)
- : Output array to store Buffer handles for the newly allocated blocks
- : Output parameter indicating actual number of blocks extended

## Dependencies
- Functions called/Symbols referenced:
  - InitLocalBuffers: Initializes local buffer system if needed
  - LimitAdditionalLocalPins: Limits extension size based on available pins
  - GetLocalVictimBuffer: Obtains buffers for new blocks
  - GetLocalBufferDescriptor: Converts buffer IDs to BufferDesc pointers
  - LocalBufHdrGetBlock: Accesses buffer data pages
  - smgrnblocks: Gets current relation size in blocks
  - InitBufferTag/hash_search: Manages local buffer hash table entries
  - smgrzeroextend: Performs physical extension of relation on disk
  - Various buffer state management functions (PinLocalBuffer, UnpinLocalBuffer, etc.)
  - I/O statistics tracking (pgstat_prepare_io_time, pgstat_count_io_op_time)
- Called from (representative examples):
  - ExtendBufferedRelCommon: Main relation extension function delegates to this for temporary relations
  - ResourceOwnerForgetBufferIO: Buffer resource management

## Notes and Other Information
- Unlike shared relations, temporary relations don't require concurrency control during extension
- All new buffer pages are zero-initialized to ensure consistent initial state
- Includes comprehensive validation to prevent extending relations beyond PostgreSQL's maximum block limit
- Handles both new buffer allocation and reuse of existing buffers for the same blocks
- Buffer state flags are carefully managed with atomic operations to maintain consistency
- I/O timing and statistics are tracked for performance monitoring of temporary relation operations
- The function ensures resource owner tracking for all pinned buffers
- Part of PostgreSQL's buffered relation extension system optimized for temporary relation performance