# ExecParallelHashTableAlloc

## Location
[src/backend/executor/nodeHash.c:3269-3288](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeHash.c#L3269-L3288)

## Overview
Allocates an empty shared memory hash table for a specific batch in a parallel hash join operation.

## Definition

```c
void
ExecParallelHashTableAlloc(HashJoinTable hashtable, int batchno)
```
## Detailed Description
This function creates an empty hash table in shared memory for a designated batch during parallel hash join execution. It allocates space for the bucket array and initializes each bucket pointer to an invalid state. The function operates within PostgreSQL's dynamic shared area (DSA) memory management system to enable multiple worker processes to access the same hash table structure concurrently.

The allocated hash table uses atomic pointers for thread-safe access across parallel workers. Each bucket is initialized with InvalidDsaPointer to indicate it contains no hash chain yet.

## Parameters / Member Variables
- `hashtable`: The HashJoinTable structure containing parallel state and batch information
- `batchno`: The batch number for which to allocate the hash table (zero-based index)
## Dependencies
- Functions called/Symbols referenced:
  - dsa_allocate
  - [dsa_get_address](../d/dsa_get_address.md)  
  - dsa_pointer_atomic_init
  - InvalidDsaPointer
- Data types used:
  - [HashJoinTable](../H/HashJoinTable.md)
  - [ParallelHashJoinBatch](../P/ParallelHashJoinBatch.md)
  - dsa_pointer_atomic
- Called from (representative examples):
  - [ExecHashTableCreate](ExecHashTableCreate.md)
  - [ExecParallelHashJoinNewBatch](ExecParallelHashJoinNewBatch.md)

## Notes and Other Information
- This function is specifically designed for parallel hash join operations and requires shared memory context
- The number of buckets is determined by the parallel_state->nbuckets value stored in the hashtable
- All bucket pointers are atomically initialized to support concurrent access by multiple worker processes
- Memory allocation failures will be handled by the underlying DSA allocation mechanism