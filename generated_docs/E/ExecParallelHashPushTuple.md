# ExecParallelHashPushTuple

## Location
[src/backend/executor/nodeHash.c:3461-3478](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeHash.c#L3461-L3478)

## Overview
A static inline function that atomically inserts a tuple at the front of a chain of tuples in DSA (Dynamic Shared Area) memory, providing thread-safe tuple insertion for parallel hash operations.

## Definition
```c
static inline void
ExecParallelHashPushTuple(dsa_pointer_atomic *head,
                         HashJoinTuple tuple,
                         dsa_pointer tuple_shared)
```

## Detailed Description
This function implements an atomic push operation for adding hash join tuples to a linked list stored in shared memory. It uses a lock-free compare-and-swap loop to ensure thread safety when multiple parallel workers are inserting tuples into the same hash bucket chain. The function continuously attempts to update the head pointer until it succeeds, handling concurrent modifications by other processes.

The implementation follows a typical lock-free stack push pattern:
1. Read the current head pointer
2. Set the new tuple's next pointer to the current head
3. Attempt to atomically update the head to point to the new tuple
4. If the compare-and-swap fails (indicating concurrent modification), retry

## Parameters / Member Variables
- `head`: Atomic pointer to the head of the tuple chain in shared memory
- `tuple`: Local pointer to the HashJoinTuple being inserted
- `tuple_shared`: Shared memory pointer (dsa_pointer) to the tuple being inserted

## Dependencies
- Functions called/Symbols referenced:
  - dsa_pointer_atomic_read
  - dsa_pointer_atomic_compare_exchange
  - dsa_pointer_atomic (type)
  - [HashJoinTuple](../H/HashJoinTuple.md) (type)
  - dsa_pointer (type)
- Called from:
  - [ExecParallelHashRepartitionFirst](ExecParallelHashRepartitionFirst.md)
  - ExecParallelHashIncreaseNumBuckets
  - ExecParallelHashTableInsert
  - ExecParallelHashTableInsertCurrentBatch

## Notes and Other Information
- This function is critical for maintaining data consistency in parallel hash join operations
- The lock-free design prevents contention and blocking between parallel workers
- The infinite loop ensures eventual success even under high contention
- Located in src/backend/executor/nodeHash.c:3461-3478