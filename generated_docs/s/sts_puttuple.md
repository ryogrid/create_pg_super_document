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

## Simplified Source

```c
void
sts_puttuple(SharedTuplestoreAccessor *accessor, void *meta_data,
             MinimalTuple tuple)
{
    size_t size = accessor->sts->meta_data_size + tuple->t_len;

    // Create write file if this is first tuple
    if (accessor->write_file == NULL)
    {
        char name[MAXPGPATH];
        sts_filename(name, accessor, accessor->participant);

        MemoryContext oldcxt = MemoryContextSwitchTo(accessor->context);
        accessor->write_file = BufFileCreateFileSet(&accessor->fileset->fs, name);
        MemoryContextSwitchTo(oldcxt);

        // Mark participant as writing
        accessor->sts->participants[accessor->participant].writing = true;
    }

    // Check if current chunk has enough space
    if (accessor->write_pointer + size > accessor->write_end)
    {
        if (accessor->write_chunk == NULL)
        {
            // Allocate first chunk
            accessor->write_chunk = (SharedTuplestoreChunk *)
                MemoryContextAllocZero(accessor->context, STS_CHUNK_PAGES * BLCKSZ);
            accessor->write_chunk->ntuples = 0;
            accessor->write_pointer = &accessor->write_chunk->data[0];
            accessor->write_end = (char *) accessor->write_chunk + STS_CHUNK_PAGES * BLCKSZ;
        }
        else
        {
            // Flush current chunk to make space
            sts_flush_chunk(accessor);
        }

        // Handle oversized tuples with overflow chunks
        if (accessor->write_pointer + size > accessor->write_end)
        {
            // Write metadata and partial tuple to current chunk
            if (accessor->sts->meta_data_size > 0)
                memcpy(accessor->write_pointer, meta_data, accessor->sts->meta_data_size);

            size_t written = accessor->write_end - accessor->write_pointer - accessor->sts->meta_data_size;
            memcpy(accessor->write_pointer + accessor->sts->meta_data_size, tuple, written);
            ++accessor->write_chunk->ntuples;

            size -= accessor->sts->meta_data_size + written;

            // Write remaining data in overflow chunks
            while (size > 0)
            {
                sts_flush_chunk(accessor);
                accessor->write_chunk->overflow = (size + STS_CHUNK_DATA_SIZE - 1) / STS_CHUNK_DATA_SIZE;

                size_t written_this_chunk = Min(accessor->write_end - accessor->write_pointer, size);
                memcpy(accessor->write_pointer, (char *) tuple + written, written_this_chunk);
                accessor->write_pointer += written_this_chunk;
                size -= written_this_chunk;
                written += written_this_chunk;
            }
            return;
        }
    }

    // Normal case: copy metadata and tuple to current chunk
    if (accessor->sts->meta_data_size > 0)
        memcpy(accessor->write_pointer, meta_data, accessor->sts->meta_data_size);
    memcpy(accessor->write_pointer + accessor->sts->meta_data_size, tuple, tuple->t_len);

    accessor->write_pointer += size;
    ++accessor->write_chunk->ntuples;
}
```