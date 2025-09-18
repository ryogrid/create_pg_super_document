# PinBufferForBlock

## Location
[src/backend/storage/buffer/bufmgr.c:1105-1197](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/buffer/bufmgr.c#L1105-L1197)

## Overview
PinBufferForBlock is a core internal function in PostgreSQL's buffer management system that pins a buffer for a given block, handling both shared and local (temporary) buffers with appropriate I/O context setup and statistics tracking.

## Definition


## Detailed Description
PinBufferForBlock is a critical function in PostgreSQL's buffer manager that handles the low-level task of pinning buffers for specific database blocks. The function determines whether to use local (temporary) or shared buffers based on relation persistence, allocates the appropriate buffer, and updates various statistics and tracing information. It sets the  flag to indicate whether the block was already present in the buffer pool or needs to be read from disk. The function is marked as  for performance optimization since it's called frequently during database operations.

## Parameters / Member Variables
- : Relation pointer, can be NULL during recovery operations
- : Storage manager relation containing relation metadata and location information
- : Persistence type when relation is NULL (used during recovery)
- : Fork identifier (main, FSM, visibility map, etc.)
- : Block number within the specified fork (must not be P_NEW)
- : Buffer access strategy for cache management policies
- : Output parameter set to true if block was found in buffer pool, false if needs reading

## Dependencies
- Functions called/Symbols referenced:
  - LocalBufferAlloc
  - [BufferAlloc](../B/BufferAlloc.md)
  - IOContextForStrategy
  - [BufferDescriptorGetBuffer](../B/BufferDescriptorGetBuffer.md)
  - pgstat_count_buffer_read
  - pgstat_count_buffer_hit
  - [pgstat_count_io_op](../p/pgstat_count_io_op.md)
- Called from (representative examples):
  - [ReadBuffer_common](../R/ReadBuffer_common.md)
  - [StartReadBuffersImpl](../S/StartReadBuffersImpl.md)

## Notes and Other Information
- The function handles different persistence types (permanent, temporary, unlogged) with appropriate I/O contexts
- Statistics are updated for both global (pgBufferUsage) and per-relation counters
- Vacuum-related statistics (VacuumPageHit, VacuumCostBalance) are maintained when blocks are found
- PostgreSQL tracing probes are included for performance monitoring
- The function asserts that blockNum is not P_NEW, as new blocks require different handling
- Local buffer allocation is used for temporary relations while shared buffer allocation is used for permanent relations