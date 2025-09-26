# SharedTuplestoreChunk

## Location
[src/backend/utils/sort/sharedtuplestore.c:42-47](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/sort/sharedtuplestore.c#L42-L47)

## Overview
SharedTuplestoreChunk is a data structure that represents a chunk of tuples written to disk as part of PostgreSQL shared tuple store implementation, used for storing tuple data efficiently in shared memory or disk-based scenarios.

## Definition
```c
typedef struct SharedTuplestoreChunk
{
    int         ntuples;        /* Number of tuples in this chunk. */
    int         overflow;       /* If overflow, how many including this one? */
    char        data[FLEXIBLE_ARRAY_MEMBER];
} SharedTuplestoreChunk;
```

## Detailed Description
SharedTuplestoreChunk serves as the fundamental storage unit for tuple data in PostgreSQL shared tuple store system. Each chunk contains metadata about the stored tuples and the actual tuple data in a flexible array member. The structure is designed to efficiently pack multiple tuples together for disk I/O operations, with overflow handling for cases where chunks exceed expected sizes. This design allows for optimal memory usage and efficient sequential access patterns when reading tuple data from disk.

## Parameters / Member Variables
- `ntuples`: The total number of tuples stored within this chunk
- `overflow`: When a chunk exceeds normal size limits, this field indicates how many chunks (including this one) are part of the overflow sequence
- `data`: Flexible array member containing the actual tuple data in serialized format

## Dependencies
- Functions called/Symbols referenced:
  - FLEXIBLE_ARRAY_MEMBER (macro for flexible array member declaration)

- Called from (representative examples):
  - STS_CHUNK_HEADER_SIZE (macro that calculates chunk header size)
  - [SharedTuplestoreAccessor](SharedTuplestoreAccessor.md) (structure that uses chunk information)
  - [sts_puttuple](../s/sts_puttuple.md) (function that writes tuples to chunks)
  - [sts_read_tuple](../s/sts_read_tuple.md) (function that reads tuples from chunks)
  - [sts_parallel_scan_next](../s/sts_parallel_scan_next.md) (function that scans chunks during parallel operations)

## Notes and Other Information
- The chunk structure is optimized for disk I/O operations in shared tuple store scenarios
- The overflow mechanism allows handling of variable-sized tuple data that may exceed normal chunk boundaries
- The flexible array member enables efficient packing of tuple data without additional pointer indirection
- This structure is fundamental to PostgreSQL parallel query processing where tuple data needs to be shared between processes