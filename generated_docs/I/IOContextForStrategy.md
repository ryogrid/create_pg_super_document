# IOContextForStrategy

## Location
[src/backend/storage/buffer/freelist.c:758-797](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/buffer/freelist.c#L758-L797)

## Overview
IOContextForStrategy is a utility function that maps a BufferAccessStrategy to its corresponding IOContext, enabling PostgreSQL to track different types of I/O operations for monitoring and statistics purposes.

## Definition

```c
IOContext
IOContextForStrategy(BufferAccessStrategy strategy)
```
## Detailed Description
This function serves as a mapping utility that translates buffer access strategies into their corresponding I/O contexts. The function examines the buffer access strategy type and returns the appropriate IOContext enum value, which is used by PostgreSQL's I/O statistics system to categorize different types of buffer operations. This mapping is crucial for monitoring and performance analysis, allowing the system to distinguish between bulk reads, bulk writes, vacuum operations, and normal operations in I/O statistics.

The function handles NULL strategy inputs by returning IOCONTEXT_NORMAL, and includes error handling for unrecognized strategy types. It contains an unreachable case for BAS_NORMAL since GetAccessStrategy() currently returns NULL for normal buffer access patterns.

## Parameters / Member Variables
- `strategy`: BufferAccessStrategy pointer that defines the buffer access pattern; can be NULL
## Dependencies
- Functions called/Symbols referenced:
  - pg_unreachable (for unreachable code paths)
  - elog (for error reporting)
- Buffer access strategy types:
  - BAS_NORMAL
  - BAS_BULKREAD  
  - BAS_BULKWRITE
  - BAS_VACUUM
- I/O context types:
  - IOCONTEXT_NORMAL
  - IOCONTEXT_BULKREAD
  - IOCONTEXT_BULKWRITE
  - IOCONTEXT_VACUUM
- Called from:
  - [PinBufferForBlock](../P/PinBufferForBlock.md)
  - [WaitReadBuffers](../W/WaitReadBuffers.md)
  - [ExtendBufferedRelShared](../E/ExtendBufferedRelShared.md)
  - [ResourceOwnerForgetBufferIO](../R/ResourceOwnerForgetBufferIO.md)

## Notes and Other Information
- The function contains defensive programming with pg_unreachable() calls and error handling for invalid strategy types
- The BAS_NORMAL case is marked as unreachable because the current implementation of GetAccessStrategy() returns NULL for normal access patterns rather than creating a BAS_NORMAL strategy
- This function is essential for PostgreSQL's I/O monitoring infrastructure, enabling detailed tracking of different operation types in system statistics
- The mapping is used throughout the buffer management system to properly categorize I/O operations for performance monitoring and analysis

## Simplified Source

```c
IOContext IOContextForStrategy(BufferAccessStrategy strategy)
{
    // Handle NULL strategy - use normal context
    if (!strategy)
        return IOCONTEXT_NORMAL;

    // Map strategy type to corresponding I/O context
    switch (strategy->btype) {
        case BAS_NORMAL:
            /*
             * Currently GetAccessStrategy() returns NULL for BAS_NORMAL
             * instead of creating a strategy object, so this case is
             * unreachable in practice.
             */
            pg_unreachable();
            return IOCONTEXT_NORMAL;

        case BAS_BULKREAD:
            return IOCONTEXT_BULKREAD;

        case BAS_BULKWRITE:
            return IOCONTEXT_BULKWRITE;

        case BAS_VACUUM:
            return IOCONTEXT_VACUUM;
    }

    // Should never reach here - unknown strategy type
    elog(ERROR, "unrecognized BufferAccessStrategyType: %d", strategy->btype);
    pg_unreachable();
}
```