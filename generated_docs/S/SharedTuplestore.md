# SharedTuplestore

## Location
src/backend/utils/sort/sharedtuplestore.c: 59 - 70

## Overview
SharedTuplestore is the main control structure that lives in shared memory for managing tuple storage operations across multiple participating processes in PostgreSQL parallel query execution.

## Definition
```c
struct SharedTuplestore
{
    int         nparticipants;  /* Number of participants that can write. */
    int         flags;          /* Flag bits from SHARED_TUPLESTORE_XXX */
    size_t      meta_data_size; /* Size of per-tuple header. */
    char        name[NAMEDATALEN];  /* A name for this tuplestore. */

    /* Followed by per-participant shared state. */
    SharedTuplestoreParticipant participants[FLEXIBLE_ARRAY_MEMBER];
};
```

## Detailed Description
SharedTuplestore serves as the central coordination structure for PostgreSQL shared tuple store system. It resides in shared memory and contains metadata about the tuple store operation, including the number of participating processes, configuration flags, and per-tuple metadata size information. The structure includes a flexible array of SharedTuplestoreParticipant structures, allowing each participating process to maintain its own state while sharing the overall tuple store. This design enables efficient parallel processing of tuple data with proper coordination and synchronization between multiple processes.

## Parameters / Member Variables
- `nparticipants`: The total number of processes that are permitted to write to this shared tuple store
- `flags`: Configuration flags controlling tuple store behavior, using SHARED_TUPLESTORE_XXX constants
- `meta_data_size`: The size in bytes of metadata headers that precede each tuple in storage
- `name`: A human-readable identifier for this tuple store instance, limited to NAMEDATALEN characters
- `participants`: Flexible array containing per-participant state structures for coordinating individual process access

## Dependencies
- Functions called/Symbols referenced:
  - NAMEDATALEN (constant defining maximum length for database object names)
  - SharedTuplestoreParticipant (structure for per-participant state)
  - FLEXIBLE_ARRAY_MEMBER (macro for flexible array member declaration)

- Called from (representative examples):
  - SharedTuplestoreAccessor (structure that provides access interface to tuple store)
  - sts_estimate (function that calculates memory requirements)
  - sts_initialize (function that initializes a new shared tuple store)
  - sts_attach (function that attaches a process to existing tuple store)
  - ParallelHashJoinBatchInner/Outer (hash join batch structures that use shared tuple stores)
  - SHARED_TUPLESTORE_SINGLE_PASS (flag constant for single-pass operation mode)

## Notes and Other Information
- The structure lives in shared memory to coordinate multiple processes in parallel query execution
- The flexible array of participants allows dynamic sizing based on the actual number of participating processes
- The flags field supports various operation modes including single-pass processing
- This structure is fundamental to PostgreSQL parallel hash joins and other parallel query operations
- The name field aids in debugging and monitoring of shared tuple store operations
- The meta_data_size field enables variable-sized tuple headers for different use cases