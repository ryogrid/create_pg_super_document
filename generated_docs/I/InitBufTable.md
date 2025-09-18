# InitBufTable

## Location
src/backend/storage/buffer/buf_table.c: 51 - 77

## Overview
Initializes the shared memory hash table used for mapping buffer tags to buffer descriptors during PostgreSQL startup.

## Definition
```c
void InitBufTable(int size)
```

## Detailed Description
InitBufTable creates and initializes the shared buffer lookup hashtable (SharedBufHash) that maps BufferTag keys to buffer descriptors. This hashtable is a critical component of the buffer management system, allowing efficient lookup of buffers based on their identifying tags (relation, fork, block number). The function sets up a partitioned hash table using ShmemInitHash with specific configuration parameters optimized for buffer management operations.

## Parameters / Member Variables
- `size`: The desired hash table size (possibly more than NBuffers) - specifies the initial and maximum number of hash buckets

## Dependencies
- Functions called/Symbols referenced:
  - ShmemInitHash
  - HASHCTL
  - BufferTag
  - BufferLookupEnt
  - NUM_BUFFER_PARTITIONS
  - HASH_ELEM
  - HASH_BLOBS
  - HASH_PARTITION
- Called from (representative examples):
  - StrategyInitialize
  - [ResourceOwnerForgetBufferIO](../R/ResourceOwnerForgetBufferIO.md)

## Notes and Other Information
The function assumes no locking is needed during initialization since it runs during system startup before concurrent access begins. The hashtable is configured with partitioning (NUM_BUFFER_PARTITIONS) to reduce lock contention in multi-process environments. The HASH_ELEM, HASH_BLOBS, and HASH_PARTITION flags specify that the table uses fixed-size elements, treats keys as binary data, and supports partitioned locking respectively.