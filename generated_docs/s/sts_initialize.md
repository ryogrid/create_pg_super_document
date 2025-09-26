# sts_initialize

## Location
[src/backend/utils/sort/sharedtuplestore.c:126-177](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/sort/sharedtuplestore.c#L126-L177)

## Overview
Initializes a SharedTuplestore structure in existing shared memory and returns an accessor for the calling participant to interact with the shared tuple store.

## Definition

```c
SharedTuplestoreAccessor *
sts_initialize(SharedTuplestore *sts, int participants,
			   int my_participant_number,
			   size_t meta_data_size,
			   int flags,
			   SharedFileSet *fileset,
			   const char *name)
```
## Detailed Description
The  function sets up a  in pre-allocated shared memory and creates a  for the calling process to interact with it. The function initializes all participant structures, sets up locks, and configures the shared tuple store with the specified parameters.

The function supports optional metadata that can be stored alongside tuples (useful for hash values in parallel hash joins) and validates that the metadata size doesn't exceed chunk limits. Each participant gets its own lock and state tracking variables initialized.

## Parameters / Member Variables
- : Pointer to the SharedTuplestore structure in shared memory to initialize
- : Total number of participants that will access this shared tuple store  
- : The participant number for this calling process (must be < participants)
- : Size of optional metadata to store with each tuple
- : Configuration flags (e.g., SHARED_TUPLESTORE_SINGLE_PASS for eager cleanup)
- : SharedFileSet that manages temporary files for the tuple store
- : Unique name for this SharedTuplestore within the SharedFileSet

## Dependencies
- Functions called/Symbols referenced:
  - [SharedTuplestore](../S/SharedTuplestore.md) (struct type)
  - SharedFileSet (struct type)  
  - [SharedTuplestoreAccessor](../S/SharedTuplestoreAccessor.md) (struct type)
  - STS_CHUNK_DATA_SIZE (constant)
  - [LWLockInitialize](../L/LWLockInitialize.md)
  - LWTRANCHE_SHARED_TUPLESTORE (constant)
- Called from (representative examples):
  - [ExecParallelHashJoinSetUpBatches](../E/ExecParallelHashJoinSetUpBatches.md)
  - SHARED_TUPLESTORE_SINGLE_PASS

## Notes and Other Information
- The function validates that metadata size plus tuple header fits within a single chunk (STS_CHUNK_DATA_SIZE)
- Each participant gets an individual LWLock for synchronization during tuple operations
- The returned accessor contains the participant number, reference to the shared structure, fileset, and current memory context
- Name length is validated against the fixed-size buffer in the SharedTuplestore structure
- This is part of PostgreSQL's infrastructure for sharing tuples between parallel workers during query execution