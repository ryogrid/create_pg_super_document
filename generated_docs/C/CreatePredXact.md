# CreatePredXact

## Location
src/backend/storage/lmgr/predicate.c: 582 - 595

## Overview
Allocates and initializes a new SERIALIZABLEXACT structure from a shared memory pool for tracking serializable transactions in PostgreSQL's SSI implementation.

## Definition
```c
static SERIALIZABLEXACT *CreatePredXact(void)
```

## Detailed Description
This function manages the allocation of SERIALIZABLEXACT structures from a pre-allocated shared memory pool. It implements a simple but efficient memory management scheme for serializable transaction tracking:

1. **Availability check**: First checks if any SERIALIZABLEXACT structures are available in the free pool
2. **Allocation**: If available, removes one structure from the available list using a FIFO approach
3. **Activation**: Moves the allocated structure to the active list to track it as an in-use serializable transaction
4. **Failure handling**: Returns NULL if no structures are available (pool exhausted)

The function operates on two shared memory doubly-linked lists:
- `PredXact->availableList`: Pool of unused SERIALIZABLEXACT structures
- `PredXact->activeList`: List of currently active serializable transactions

This design provides O(1) allocation and deallocation performance while maintaining a fixed-size pool of transaction structures in shared memory.

## Parameters / Member Variables
- No parameters (operates on global shared memory structures)

## Dependencies
- Functions called/Symbols referenced:
  - SERIALIZABLEXACT (the main serializable transaction structure type)
  - dlist_is_empty (function to check if doubly-linked list is empty)
  - dlist_container (macro to get containing structure from list node)
  - dlist_pop_head_node (function to remove and return head node from list)
  - dlist_push_tail (function to add node to tail of list)
- Called from (representative examples):
  - SerialControl
  - InitPredicateLocks
  - GetSerializableTransactionSnapshotInt
  - predicatelock_twophase_recover

## Notes and Other Information
- Returns NULL when the pool of available SERIALIZABLEXACT structures is exhausted
- Uses a simple FIFO allocation strategy for cache-friendly behavior
- Part of PostgreSQL's shared memory management for serializable transaction tracking
- The comment indicates this could be replaced with a generalized shared memory list implementation in the future
- Critical for managing the fixed-size pool of serializable transaction slots in shared memory
- Allocation failure (NULL return) can limit the number of concurrent serializable transactions