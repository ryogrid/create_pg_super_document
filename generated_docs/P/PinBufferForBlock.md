# PinBufferForBlock

## Location
[src/backend/storage/buffer/bufmgr.c:1105-1197](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/buffer/bufmgr.c#L1105-L1197)

## Overview
PinBufferForBlock is a core internal function in PostgreSQL's buffer management system that pins a buffer for a given block, handling both shared and local (temporary) buffers with appropriate I/O context setup and statistics tracking.

## Definition

```c
static pg_attribute_always_inline Buffer
PinBufferForBlock(Relation rel,
				  SMgrRelation smgr,
				  char smgr_persistence,
				  ForkNumber forkNum,
				  BlockNumber blockNum,
				  BufferAccessStrategy strategy,
				  bool *foundPtr)
```
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
  - [LocalBufferAlloc](../L/LocalBufferAlloc.md)
  - [BufferAlloc](../B/BufferAlloc.md)
  - [IOContextForStrategy](../I/IOContextForStrategy.md)
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

## Simplified Source

```c
static pg_attribute_always_inline Buffer
PinBufferForBlock(Relation rel,
                  SMgrRelation smgr,
                  char smgr_persistence,
                  ForkNumber forkNum,
                  BlockNumber blockNum,
                  BufferAccessStrategy strategy,
                  bool *foundPtr)
{
    BufferDesc *bufHdr;
    IOContext io_context;
    IOObject io_object;
    char persistence;

    // Determine relation persistence type
    if (rel)
        persistence = rel->rd_rel->relpersistence;
    else if (smgr_persistence == 0)
        persistence = RELPERSISTENCE_PERMANENT;
    else
        persistence = smgr_persistence;

    // Set I/O context based on persistence
    if (persistence == RELPERSISTENCE_TEMP) {
        io_context = IOCONTEXT_NORMAL;
        io_object = IOOBJECT_TEMP_RELATION;
    } else {
        io_context = IOContextForStrategy(strategy);
        io_object = IOOBJECT_RELATION;
    }

    // Start buffer read tracing
    TRACE_POSTGRESQL_BUFFER_READ_START(forkNum, blockNum, /* relation info */);

    // Allocate buffer based on persistence
    if (persistence == RELPERSISTENCE_TEMP) {
        bufHdr = LocalBufferAlloc(smgr, forkNum, blockNum, foundPtr);
        if (*foundPtr)
            pgBufferUsage.local_blks_hit++;
    } else {
        bufHdr = BufferAlloc(smgr, persistence, forkNum, blockNum,
                            strategy, foundPtr, io_context);
        if (*foundPtr)
            pgBufferUsage.shared_blks_hit++;
    }

    // Update per-relation statistics
    if (rel) {
        pgstat_count_buffer_read(rel);
        if (*foundPtr)
            pgstat_count_buffer_hit(rel);
    }

    // Update vacuum and I/O statistics for cache hits
    if (*foundPtr) {
        VacuumPageHit++;
        pgstat_count_io_op(io_object, io_context, IOOP_HIT);
        if (VacuumCostActive)
            VacuumCostBalance += VacuumCostPageHit;

        // Complete buffer read tracing
        TRACE_POSTGRESQL_BUFFER_READ_DONE(/* relation info */, true);
    }

    return BufferDescriptorGetBuffer(bufHdr);
}
```