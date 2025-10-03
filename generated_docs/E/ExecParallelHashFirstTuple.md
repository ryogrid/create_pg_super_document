# ExecParallelHashFirstTuple

## Location
[src/backend/executor/nodeHash.c:3431-3446](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeHash.c#L3431-L3446)

## Overview
Retrieves the first tuple from a specified bucket in a parallel hash table using thread-safe atomic operations.

## Definition

```c
static inline HashJoinTuple
ExecParallelHashFirstTuple(HashJoinTable hashtable, int bucketno)
```
## Detailed Description
This function provides thread-safe access to the first tuple in a hash table bucket during parallel hash join execution. It uses atomic pointer operations to safely read the bucket head pointer from shared memory and then converts the DSA pointer to a local memory address. The function is designed to work with parallel hash joins where multiple worker processes may be concurrently accessing the same hash table structure.

The function operates on shared hash table buckets, which are implemented as arrays of atomic pointers. Each bucket contains a linked list of hash join tuples, and this function retrieves the head of that list. The atomic read operation ensures that the pointer value is read consistently even when other processes might be modifying the bucket concurrently.

## Parameters / Member Variables
- `hashtable`: The HashJoinTable structure containing the shared bucket array and DSA area
- `bucketno`: The bucket number (zero-based index) from which to retrieve the first tuple
## Dependencies
- Functions called/Symbols referenced:
  - dsa_pointer_atomic_read
  - [dsa_get_address](../d/dsa_get_address.md)
- Data types used:
  - [HashJoinTable](../H/HashJoinTable.md)
  - [HashJoinTuple](../H/HashJoinTuple.md)
  - dsa_pointer
- Called from (representative examples):
  - [ExecParallelScanHashBucket](ExecParallelScanHashBucket.md)
  - [ExecParallelScanHashTableForUnmatched](ExecParallelScanHashTableForUnmatched.md)

## Notes and Other Information
- This is a static inline function for optimal performance in tight scanning loops
- Requires that hashtable->parallel_state is not NULL (verified by assertion)
- Uses atomic operations to ensure thread safety in parallel execution
- Returns NULL if the bucket is empty (when DSA pointer is invalid)
- The function assumes the bucket number is valid and within the hash table bounds
- Part of the parallel hash join tuple scanning infrastructure