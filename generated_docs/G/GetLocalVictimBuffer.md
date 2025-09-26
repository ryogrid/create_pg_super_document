# GetLocalVictimBuffer

## Location
src/backend/storage/buffer/localbuf.c: 177 - 289

## Overview
GetLocalVictimBuffer selects and prepares a local buffer for reuse using a clock sweep algorithm, handling dirty page write-out and buffer state transitions as needed.

## Definition


## Detailed Description
GetLocalVictimBuffer implements buffer replacement policy for local buffers using a clock sweep algorithm similar to the main buffer manager. The function searches for an unpinned buffer with zero usage count, decrementing usage counts as it encounters buffers that are still 'warm' in the cache. When a suitable victim is found, it handles several critical tasks:

1. **Lazy allocation**: Allocates physical storage for the buffer if not already done
2. **Dirty page handling**: Writes dirty pages to disk before reusing the buffer, including checksum calculation and I/O statistics tracking
3. **Hash table maintenance**: Removes the old buffer tag from the local buffer hash table if it was valid
4. **State cleanup**: Clears buffer flags and resets the buffer to an invalid state

The function ensures resource ownership tracking and includes safety checks to prevent corruption of the local buffer hash table.

## Parameters
None (static function with no parameters)

## Dependencies
- Functions called/Symbols referenced:
  - ResourceOwnerEnlarge: Ensures resource owner can track additional buffer
  - GetLocalBufferDescriptor: Converts buffer ID to BufferDesc pointer
  - pg_atomic_read_u32/pg_atomic_unlocked_write_u32: Atomic state operations
  - PinLocalBuffer: Pins the selected victim buffer
  - LocalBufHdrGetBlock: Gets/sets the buffer's data page pointer
  - GetLocalBufferStorage: Allocates physical storage for buffer
  - smgropen: Opens storage manager relation for dirty page write-out
  - PageSetChecksumInplace: Calculates and sets page checksum before writing
  - smgrwrite: Performs actual disk write of dirty page
  - hash_search: Removes old buffer tag from hash table
  - ClearBufferTag/BufferDescriptorGetBuffer: Buffer tag and descriptor utilities
  - Various I/O statistics functions (pgstat_prepare_io_time, pgstat_count_io_op_time, etc.)
- Called from (representative examples):
  - LocalBufferAlloc: Uses this to get victim when allocating new local buffer
  - ExtendBufferedRelLocal: Uses this when extending buffered relations locally

## Notes and Other Information
- Implements clock sweep replacement algorithm with usage count-based aging
- Uses lazy memory allocation - physical storage allocated only on first use
- Handles dirty page write-out with proper checksumming and I/O timing statistics
- Includes error handling for resource exhaustion (no available buffers)
- Tracks buffer usage statistics for temporary relation I/O operations
- Buffer state transitions are handled atomically to maintain consistency
- The trycounter mechanism prevents infinite loops when all buffers are pinned
- Part of PostgreSQL's local buffer management optimized for temporary relations performance