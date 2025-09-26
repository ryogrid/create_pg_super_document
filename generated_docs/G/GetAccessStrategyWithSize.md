# GetAccessStrategyWithSize

## Location
[src/backend/storage/buffer/freelist.c:584-623](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/buffer/freelist.c#L584-L623)

## Overview
Creates a BufferAccessStrategy object with a specific ring buffer size in kilobytes, providing fine-grained control over buffer ring allocation.

## Definition
```c
BufferAccessStrategy GetAccessStrategyWithSize(BufferAccessStrategyType btype, int ring_size_kb)
```

## Detailed Description
GetAccessStrategyWithSize creates a customized BufferAccessStrategy with a user-specified ring size. It converts the requested size from kilobytes to buffer count based on PostgreSQL's block size (BLCKSZ), applies safety limits to prevent excessive memory usage, and allocates the strategy structure. The function implements important safeguards: it caps the ring size to 1/8th of shared_buffers to prevent buffer cache monopolization, and returns NULL for zero-sized rings to indicate standard buffer management should be used.

The allocated structure includes the strategy metadata and a flexible array of Buffer elements sized to the calculated ring buffer count. This provides a more controlled approach compared to GetAccessStrategy's predefined sizes.

## Parameters / Member Variables
- `btype`: BufferAccessStrategyType enum specifying the access pattern type
- `ring_size_kb`: Requested ring buffer size in kilobytes (must be non-negative, 0 means no ring buffer)

## Dependencies
- Functions called/Symbols referenced:
  - BufferAccessStrategyType (enum type for strategy types)
  - [BufferAccessStrategy](../B/BufferAccessStrategy.md) (strategy object type)
  - [BufferAccessStrategyData](../B/BufferAccessStrategyData.md) (internal structure definition)
  - BLCKSZ (PostgreSQL block size constant)
  - NBuffers (global shared buffer count)
  - Min (minimum value macro)
  - pallo0 (zero-initialized memory allocator)
  - offsetof (structure offset calculation)
  - Buffer (buffer type)
- Called from (representative examples):
  - [ExecVacuum](../E/ExecVacuum.md) (src/backend/commands/vacuum.c:444)
  - [parallel_vacuum_main](../p/parallel_vacuum_main.md) (src/backend/commands/vacuumparallel.c:1067)
  - [do_autovacuum](../d/do_autovacuum.md) (src/backend/postmaster/autovacuum.c:2264)
  - [GetAccessStrategy](GetAccessStrategy.md) (src/backend/storage/buffer/freelist.c:573)

## Notes and Other Information
- Ring size is automatically capped to 1/8th of shared_buffers to prevent cache monopolization
- Returns NULL if ring_size_kb is 0, indicating no special buffer ring is needed
- Uses palloc0() to ensure all structure fields start as zero-initialized
- Converts kilobytes to buffer count using BLCKSZ/1024 division
- The structure uses a flexible array member for the buffer ring storage
- Asserts ensure ring_size_kb is non-negative and resulting ring_buffers is positive