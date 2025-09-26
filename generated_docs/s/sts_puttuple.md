# sts_puttuple

## Location
[src/backend/utils/sort/sharedtuplestore.c:300-414](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/sort/sharedtuplestore.c#L300-L414)

## Overview
Writes a tuple along with optional metadata to a shared tuplestore, handling file creation, buffer management, and oversized tuple overflow.

## Definition
void sts_puttuple(SharedTuplestoreAccessor *accessor, void *meta_data, MinimalTuple tuple)

## Detailed Description
This function writes a tuple to the shared tuplestore associated with the given accessor. It performs several key operations:

1. **File Creation**: If this is the first write for this backend, it creates a dedicated write file using BufFileCreateFileSet() and marks the participant as writing.

2. **Buffer Management**: Manages write chunks (STS_CHUNK_PAGES * BLCKSZ in size) to buffer tuples before writing them to disk. If the current chunk lacks space, it flushes the existing chunk and prepares for the new tuple.

3. **Oversized Tuple Handling**: For tuples that exceed chunk capacity, it implements a sophisticated overflow mechanism:
   - Writes metadata and the beginning of the tuple in the current chunk
   - Writes the remainder in overflow chunks
   - Sets overflow counters to help readers skip multiple overflow chunks efficiently

4. **Data Layout**: Stores metadata first (if specified), followed by the tuple data, maintaining proper alignment and size tracking.

## Parameters / Member Variables
- `accessor`: A pointer to the SharedTuplestoreAccessor that manages write operations to the shared tuplestore
- `meta_data`: A pointer to metadata of the size specified during sts_initialize(); must be provided if meta_data_size > 0
- `tuple`: A MinimalTuple containing the tuple data to be written

## Dependencies
- Functions called/Symbols referenced:
  - [SharedTuplestoreAccessor](../S/SharedTuplestoreAccessor.md), SharedTuplestoreParticipant, SharedTuplestoreChunk (structure types)
  - MinimalTuple (tuple type)
  - [sts_filename](sts_filename.md) (function to generate filename)
  - [BufFileCreateFileSet](../B/BufFileCreateFileSet.md) (function to create buffer file)
  - [sts_flush_chunk](sts_flush_chunk.md) (function to flush chunks to disk)
  - [MemoryContextAllocZero](../M/MemoryContextAllocZero.md) (memory allocation function)
  - STS_CHUNK_PAGES, STS_CHUNK_DATA_SIZE (constants)
- Called from (representative examples):
  - [ExecParallelHashRepartitionFirst](../E/ExecParallelHashRepartitionFirst.md) (in nodeHash.c:1356)
  - [ExecParallelHashRepartitionRest](../E/ExecParallelHashRepartitionRest.md) (in nodeHash.c:1424)
  - [ExecParallelHashTableInsert](../E/ExecParallelHashTableInsert.md) (in nodeHash.c:1771)
  - [ExecParallelHashJoinPartitionOuter](../E/ExecParallelHashJoinPartitionOuter.md) (in nodeHashjoin.c:1529)

## Notes and Other Information
- Creates write files lazily on first tuple write to avoid unnecessary file creation
- Efficiently handles both normal-sized and oversized tuples using overflow chunk mechanism
- Maintains tuple count in each chunk for efficient reading operations
- Uses memory context switching to ensure proper memory management
- Metadata size must match the size specified during sts_initialize()
- Critical component of PostgreSQLs parallel hash join and repartitioning operations
- Ensures thread-safe writing by having each backend write to its own file