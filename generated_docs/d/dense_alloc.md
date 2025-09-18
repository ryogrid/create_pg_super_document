# dense_alloc

## Location
src/backend/executor/nodeHash.c: 2876 - 2955

## Overview
Allocates memory for hash table tuples from memory chunks, managing allocation of both normal-sized and oversized tuples within the HashJoinTable's memory context.

## Definition


## Detailed Description
The  function is a memory allocation utility specifically designed for hash join operations in PostgreSQL. It manages memory allocation for hash table tuples using a chunked memory allocation strategy to optimize memory usage and reduce fragmentation.

The function handles two types of allocations:
1. **Large allocations**: For tuples exceeding , it creates dedicated chunks to avoid wasting space in regular chunks
2. **Regular allocations**: For normal-sized tuples, it uses the current active chunk or allocates a new standard-sized chunk if needed

The allocation strategy maintains a linked list of memory chunks, with the most recently allocated chunk kept at the head of the list for efficient access. This approach helps minimize memory overhead while providing fast allocation for hash join operations.

## Parameters / Member Variables
- : HashJoinTable structure containing the memory context and chunk management information
- : Size of memory to allocate in bytes (automatically aligned to MAXALIGN boundary)

## Dependencies
- Functions called/Symbols referenced:
  - MAXALIGN (for memory alignment)
  - MemoryContextAlloc (for chunk allocation from batchCxt)
  - HASH_CHUNK_DATA (macro to access chunk data area)
  - HASH_CHUNK_THRESHOLD (size threshold for large allocations)
  - HASH_CHUNK_HEADER_SIZE (header size for chunks)
  - HASH_CHUNK_SIZE (standard chunk size)
- Called from:
  - ExecHashIncreaseNumBatches (nodeHash.c:1019)
  - ExecHashTableInsert (nodeHash.c:1657)
  - ExecHashRemoveNextSkewBucket (nodeHash.c:2675)

## Notes and Other Information
- This is a static function internal to nodeHash.c, used exclusively for hash join memory management
- The function ensures proper memory alignment using MAXALIGN
- Large tuples get their own dedicated chunks to avoid fragmenting regular chunks
- The chunk list is managed as a singly-linked list with new chunks added at the head
- Memory is allocated from the hashtable's batch memory context (batchCxt)
- The function tracks both used space and tuple count within each chunk for memory management purposes