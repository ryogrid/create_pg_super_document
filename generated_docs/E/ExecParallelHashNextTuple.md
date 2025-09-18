# ExecParallelHashNextTuple

## Location
src/backend/executor/nodeHash.c: 3447 - 3460

## Overview
Retrieves the next tuple in a hash bucket chain during parallel hash join execution using shared memory pointers.

## Definition


## Detailed Description
This function provides the mechanism to traverse a linked list of hash join tuples within a bucket during parallel hash join execution. It takes the current tuple and returns the next tuple in the same hash bucket chain by following the shared memory pointer stored in the current tuple's next field. The function operates on shared memory structures and converts DSA (Dynamic Shared Area) pointers to local memory addresses for access by the current process.

This function is essential for iterating through all tuples that hash to the same bucket value during hash join processing. It works in conjunction with ExecParallelHashFirstTuple to enable complete traversal of bucket chains in a thread-safe manner suitable for parallel execution.

## Parameters / Member Variables
- : The HashJoinTable structure containing the DSA area for pointer resolution
- : The current HashJoinTuple from which to retrieve the next tuple in the chain

## Dependencies
- Functions called/Symbols referenced:
  - [dsa_get_address](../d/dsa_get_address.md)
- Data types used:
  - [HashJoinTable](../H/HashJoinTable.md)
  - [HashJoinTuple](../H/HashJoinTuple.md)
- Called from (representative examples):
  - [ExecParallelScanHashBucket](ExecParallelScanHashBucket.md)
  - [ExecParallelScanHashTableForUnmatched](ExecParallelScanHashTableForUnmatched.md)

## Notes and Other Information
- This is a static inline function for optimal performance in tuple scanning loops
- Requires that hashtable->parallel_state is not NULL (verified by assertion)
- Returns NULL when reaching the end of the bucket chain (when next.shared is InvalidDsaPointer)
- Uses shared memory pointers to maintain consistency across parallel worker processes
- The function assumes the input tuple is valid and properly initialized
- Part of the core parallel hash join tuple iteration infrastructure
- Works exclusively with shared memory structures in parallel execution contexts