# ReadBuffer_common

## Location
[src/backend/storage/buffer/bufmgr.c:1198-1256](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/buffer/bufmgr.c#L1198-L1256)

## Overview
ReadBuffer_common is the central unified function that implements the common logic for all PostgreSQL ReadBuffer variants, handling different read modes and delegating to appropriate buffer management functions.

## Definition


## Detailed Description
ReadBuffer_common serves as the core implementation for all buffer reading operations in PostgreSQL. It handles multiple read modes including extending relations with P_NEW blocks, zero-and-lock operations, and standard buffer reads with optional zero-on-error behavior. The function uses different code paths based on the requested mode: for P_NEW blocks it delegates to ExtendBufferedRel, for zero-and-lock modes it uses PinBufferForBlock followed by ZeroAndLockBuffer, and for standard reads it uses the asynchronous StartReadBuffer/WaitReadBuffers pattern. This unified approach ensures consistent behavior across all buffer reading operations while optimizing for different usage patterns.

## Parameters / Member Variables
- : Relation pointer, optional unless using P_NEW block number
- : Storage manager relation (required parameter)
- : Persistence type when relation is NULL
- : Fork identifier (main, FSM, visibility map, etc.)
- : Block number to read, or P_NEW to extend the relation
- : Read buffer mode controlling locking and initialization behavior
- : Buffer access strategy for cache management policies

## Dependencies
- Functions called/Symbols referenced:
  - [ExtendBufferedRel](../E/ExtendBufferedRel.md)
  - [PinBufferForBlock](../P/PinBufferForBlock.md)
  - [ZeroAndLockBuffer](../Z/ZeroAndLockBuffer.md)
  - [StartReadBuffer](../S/StartReadBuffer.md)
  - [WaitReadBuffers](../W/WaitReadBuffers.md)
  - BMR_REL
- Called from (representative examples):
  - [ReadBufferExtended](ReadBufferExtended.md)
  - [ReadBufferWithoutRelcache](ReadBufferWithoutRelcache.md)
  - [ExtendBufferedRelTo](../E/ExtendBufferedRelTo.md)
  - BufferIsPinned

## Notes and Other Information
- The function includes a backward compatibility path for P_NEW blocks, though ExtendBufferedRel is recommended for better scalability
- Different read modes are handled with optimized code paths: RBM_ZERO_AND_LOCK/RBM_ZERO_AND_CLEANUP_LOCK use immediate pinning and locking, while standard reads use asynchronous I/O
- The EB_SKIP_EXTENSION_LOCK flag is used for P_NEW operations to maintain compatibility with existing code
- RBM_ZERO_ON_ERROR mode translates to READ_BUFFERS_ZERO_ON_ERROR flags for the asynchronous read operations
- The function is marked as always_inline for performance since it's a critical path in database operations
- For zero-and-lock modes, there's no difference between exclusive and cleanup-strength locks since no other process can access the page contents yet