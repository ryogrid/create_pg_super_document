# sts_attach

## Location
[src/backend/utils/sort/sharedtuplestore.c:178-195](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/sort/sharedtuplestore.c#L178-L195)

## Overview
Attaches to an existing SharedTuplestore that was initialized by another backend, allowing this backend to read and write tuples to the shared structure.

## Definition

```c
SharedTuplestoreAccessor *
sts_attach(SharedTuplestore *sts,
		   int my_participant_number,
		   SharedFileSet *fileset)
```
## Detailed Description
The  function allows a backend process to connect to a  that has already been initialized by another process. Unlike , this function does not set up the shared structure itself but simply creates a new  that provides access to the existing shared tuple store.

This function is commonly used in parallel query execution where one process (typically the leader) initializes the shared tuple store and other worker processes attach to it to participate in tuple sharing operations.

## Parameters / Member Variables
- : Pointer to the already-initialized SharedTuplestore structure in shared memory
- : The participant number for this backend (must be < sts->nparticipants)
- : SharedFileSet that manages temporary files for the tuple store

## Dependencies
- Functions called/Symbols referenced:
  - SharedTuplestore (struct type)
  - SharedFileSet (struct type)
  - SharedTuplestoreAccessor (struct type)
- Called from (representative examples):
  - [ExecParallelHashRepartitionRest](../E/ExecParallelHashRepartitionRest.md)
  - [ExecParallelHashEnsureBatchAccessors](../E/ExecParallelHashEnsureBatchAccessors.md)
  - SHARED_TUPLESTORE_SINGLE_PASS

## Notes and Other Information
- This function is simpler than  as it doesn't perform initialization of the shared structure
- The participant number must be valid (less than the number of participants established during initialization)
- The returned accessor allows the backend to perform tuple operations on the shared store
- This is part of PostgreSQL's parallel execution infrastructure, commonly used in parallel hash joins where multiple workers need to share tuples
- The accessor maintains a reference to the current memory context for proper cleanup