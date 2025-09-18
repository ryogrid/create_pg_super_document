# BufTableShmemSize

## Location
src/backend/storage/buffer/buf_table.c: 41 - 50

## Overview
Estimates the shared memory size needed for the buffer mapping hashtable based on the desired hashtable size.

## Definition
```c
Size BufTableShmemSize(int size)
```

## Detailed Description
BufTableShmemSize calculates the amount of shared memory required to allocate a hashtable for buffer mapping operations. This function is used during PostgreSQL initialization to determine memory requirements before the actual hashtable is created. It leverages the generic hash_estimate_size function to compute the memory footprint based on the desired hashtable size and the size of BufferLookupEnt entries.

## Parameters / Member Variables
- `size`: The desired hash table size (possibly more than NBuffers) - specifies how many hash buckets the table should accommodate

## Dependencies
- Functions called/Symbols referenced:
  - hash_estimate_size
  - BufferLookupEnt
- Called from (representative examples):
  - StrategyShmemSize
  - ResourceOwnerForgetBufferIO

## Notes and Other Information
This function is part of the buffer management subsystem initialization process. The estimated size is used to allocate the appropriate amount of shared memory for the buffer lookup hashtable before the actual hashtable structure is initialized. The size parameter may be larger than NBuffers to provide room for hash collisions and maintain good performance characteristics.