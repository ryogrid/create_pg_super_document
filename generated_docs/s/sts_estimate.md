# sts_estimate

## Location
[src/backend/utils/sort/sharedtuplestore.c:104-125](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/sort/sharedtuplestore.c#L104-L125)

## Overview
Calculates the amount of shared memory required to allocate a SharedTuplestore for a given number of participants.

## Definition

```c
size_t
sts_estimate(int participants)
```
## Detailed Description
The  function provides a memory size calculation for allocating a  structure in shared memory. It computes the total memory needed by adding the base size of the  structure up to the participants field (using ) plus the memory required for an array of  structures, one for each participant.

This function is typically used during parallel query planning to determine how much shared memory needs to be allocated for tuple sharing between parallel workers.

## Parameters / Member Variables
- : The number of participants (parallel workers) that will access the shared tuple store

## Dependencies
- Functions called/Symbols referenced:
  - [SharedTuplestore](../S/SharedTuplestore.md) (struct type)
  - [SharedTuplestoreParticipant](../S/SharedTuplestoreParticipant.md) (struct type)
  - [SharedTuplestoreAccessor](../S/SharedTuplestoreAccessor.md) (struct type)
- Called from (representative examples):
  - ParallelHashJoinBatchOuter
  - EstimateParallelHashJoinBatch
  - SHARED_TUPLESTORE_SINGLE_PASS

## Notes and Other Information
- This function is part of the shared tuple store infrastructure used for parallel query execution
- The memory calculation uses  to ensure proper alignment and avoid including the flexible array member in the base calculation
- The returned size is used by PostgreSQL's shared memory allocation routines to reserve appropriate space before initializing the shared tuple store